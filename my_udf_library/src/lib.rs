use scylla_udf::export_udf;

type SmallInt = i16;

#[export_udf]
fn add(i1: SmallInt, i2: SmallInt) -> SmallInt {
    i1 + i2
}

/*
pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
*/
