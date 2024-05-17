use super::OpIterator;
use crate::Managers;
use common::bytecode_expr::ByteCodeExpr;
use common::{CrustyError, Field, TableSchema, Tuple};
use std::collections::HashMap;

/// Hash equi-join implementation. (You can add any other fields that you think are neccessary)
pub struct HashEqJoin {
    // Static objects (No need to reset on close)
    #[allow(dead_code)]
    managers: &'static Managers,

    // Parameters (No need to reset on close)
    schema: TableSchema,
    left_expr: ByteCodeExpr,
    right_expr: ByteCodeExpr,
    left_child: Box<dyn OpIterator>,
    right_child: Box<dyn OpIterator>,
    // States (Need to reset on close)
    // todo!("Your code here")
    open: bool,
    current_tuple: Option<Tuple>,
    outer_hash: HashMap<Field, Vec<Tuple>>,
}

impl HashEqJoin {
    /// NestedLoopJoin constructor. Creates a new node for a nested-loop join.
    ///
    /// # Arguments
    ///
    /// * `left_expr` - ByteCodeExpr for the left field in join condition.
    /// * `right_expr` - ByteCodeExpr for the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    pub fn new(
        managers: &'static Managers,
        schema: TableSchema,
        left_expr: ByteCodeExpr,
        right_expr: ByteCodeExpr,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
    ) -> Self {
        Self {
            managers,
            schema,
            left_expr,
            right_expr,
            left_child,
            right_child,
            open: false,
            current_tuple: None,
            outer_hash: HashMap::new(),
        }
    }
}

impl OpIterator for HashEqJoin {
    fn configure(&mut self, will_rewind: bool) {
        self.left_child.configure(false); // left child will never be rewound by HJ
        self.right_child.configure(will_rewind);
    }

    fn open(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            self.left_child.open()?;
            self.right_child.open()?;
            self.current_tuple = self.right_child.next()?;
            self.open = true;
            while let Some(left_tuple) = self.left_child.next()? {
                let left = self.left_expr.eval(&left_tuple);
                if let Some(cur_tuples) = self.outer_hash.get(&left) {
                    // If more than one tuple matches to the same expression
                    let mut new_tuples = cur_tuples.clone();
                    new_tuples.push(left_tuple);
                    self.outer_hash.insert(left, new_tuples);
                } else {
                    self.outer_hash.insert(left, vec![left_tuple.clone()]);
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::never_loop)]
    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Iterator is not open");
        }
        // Iterate through all right tuples (inner table)
        while let Some(right_tuple) = &self.current_tuple {
            let right = self.right_expr.eval(right_tuple);
            // If the left key exists using the right value,
            // merge the tuples together
            if let Some(left_val) = self.outer_hash.get(&right) {
                for (idx, left_tuple) in left_val.iter().enumerate() {
                    let t = left_tuple.merge(right_tuple);
                    if idx == (left_val.len() - 1) {
                        self.current_tuple = self.right_child.next()?;
                    }
                    return Ok(Some(t));
                }
            }
            self.current_tuple = self.right_child.next()?;
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        self.left_child.close()?;
        self.right_child.close()?;
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        self.right_child.rewind()?;
        self.current_tuple = self.right_child.next()?;
        Ok(())
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::super::TupleIterator;
    use super::*;
    use crate::testutil::execute_iter;
    use crate::testutil::new_test_managers;
    use crate::testutil::TestTuples;
    use common::bytecode_expr::{ByteCodeExpr, ByteCodes};

    fn get_join_predicate() -> (ByteCodeExpr, ByteCodeExpr) {
        // Joining two tables each containing the following tuples:
        // 1 1 3 E
        // 2 1 3 G
        // 3 1 4 A
        // 4 2 4 G
        // 5 2 5 G
        // 6 2 5 G

        // left(col(0) + col(1)) OP right(col(2))
        let mut left = ByteCodeExpr::new();
        left.add_code(ByteCodes::PushField as usize);
        left.add_code(0);
        left.add_code(ByteCodes::PushField as usize);
        left.add_code(1);
        left.add_code(ByteCodes::Add as usize);

        let mut right = ByteCodeExpr::new();
        right.add_code(ByteCodes::PushField as usize);
        right.add_code(2);

        (left, right)
    }

    fn get_iter(left_expr: ByteCodeExpr, right_expr: ByteCodeExpr) -> Box<dyn OpIterator> {
        let setup = TestTuples::new("");
        let managers = new_test_managers();
        let mut iter = Box::new(HashEqJoin::new(
            managers,
            setup.schema.clone(),
            left_expr,
            right_expr,
            Box::new(TupleIterator::new(
                setup.tuples.clone(),
                setup.schema.clone(),
            )),
            Box::new(TupleIterator::new(
                setup.tuples.clone(),
                setup.schema.clone(),
            )),
        ));
        iter.configure(false);
        iter
    }

    fn run_hash_eq_join(left_expr: ByteCodeExpr, right_expr: ByteCodeExpr) -> Vec<Tuple> {
        let mut iter = get_iter(left_expr, right_expr);
        execute_iter(&mut *iter, true).unwrap()
    }

    mod hash_eq_join_test {
        use super::*;

        #[test]
        #[should_panic]
        fn test_empty_predicate_join() {
            let left_expr = ByteCodeExpr::new();
            let right_expr = ByteCodeExpr::new();
            let _ = run_hash_eq_join(left_expr, right_expr);
        }

        #[test]
        fn test_join() {
            // Joining two tables each containing the following tuples:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G

            // left(col(0) + col(1)) == right(col(2))

            // Output:
            // 2 1 3 G 1 1 3 E
            // 2 1 3 G 2 1 3 G
            // 3 1 4 A 3 1 4 A
            // 3 1 4 A 4 2 4 G
            let (left_expr, right_expr) = get_join_predicate();
            let t = run_hash_eq_join(left_expr, right_expr);
            assert_eq!(t.len(), 4);
            assert_eq!(
                t[0],
                Tuple::new(vec![
                    Field::Int(2),
                    Field::Int(1),
                    Field::Int(3),
                    Field::String("G".to_string()),
                    Field::Int(1),
                    Field::Int(1),
                    Field::Int(3),
                    Field::String("E".to_string()),
                ])
            );
            assert_eq!(
                t[1],
                Tuple::new(vec![
                    Field::Int(2),
                    Field::Int(1),
                    Field::Int(3),
                    Field::String("G".to_string()),
                    Field::Int(2),
                    Field::Int(1),
                    Field::Int(3),
                    Field::String("G".to_string()),
                ])
            );
            assert_eq!(
                t[2],
                Tuple::new(vec![
                    Field::Int(3),
                    Field::Int(1),
                    Field::Int(4),
                    Field::String("A".to_string()),
                    Field::Int(3),
                    Field::Int(1),
                    Field::Int(4),
                    Field::String("A".to_string()),
                ])
            );
            assert_eq!(
                t[3],
                Tuple::new(vec![
                    Field::Int(3),
                    Field::Int(1),
                    Field::Int(4),
                    Field::String("A".to_string()),
                    Field::Int(4),
                    Field::Int(2),
                    Field::Int(4),
                    Field::String("G".to_string()),
                ])
            );
        }
    }

    mod opiterator_test {
        use super::*;
        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(left_expr, right_expr);
            let _ = iter.next();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(left_expr, right_expr);
            let _ = iter.rewind();
        }

        #[test]
        fn test_open() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(left_expr, right_expr);
            iter.open().unwrap();
        }

        #[test]
        fn test_close() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(left_expr, right_expr);
            iter.open().unwrap();
            iter.close().unwrap();
        }

        #[test]
        fn test_rewind() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(left_expr, right_expr);
            iter.configure(true);
            let t_before = execute_iter(&mut *iter, false).unwrap();
            iter.rewind().unwrap();
            let t_after = execute_iter(&mut *iter, false).unwrap();
            assert_eq!(t_before, t_after);
        }
    }
}
