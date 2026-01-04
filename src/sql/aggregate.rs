//! Aggregation logic

use crate::sql::expr::compare_values;
use crate::types::Value;
use anyhow::{anyhow, Result};

#[derive(Debug)]
pub enum Aggregator {
    Count(i64),
    Sum(Value),
    Max(Value),
    Min(Value),
    Avg { sum: f64, count: i64 },
}

impl Aggregator {
    pub fn new(kind: &str) -> Result<Self> {
        match kind.to_uppercase().as_str() {
            "COUNT" => Ok(Aggregator::Count(0)),
            "SUM" => Ok(Aggregator::Sum(Value::Null)),
            "MAX" => Ok(Aggregator::Max(Value::Null)),
            "MIN" => Ok(Aggregator::Min(Value::Null)),
            "AVG" => Ok(Aggregator::Avg { sum: 0.0, count: 0 }),
            _ => Err(anyhow!("Unsupported aggregate function: {}", kind)),
        }
    }

    pub fn update(&mut self, val: &Value) -> Result<()> {
        match self {
            Aggregator::Count(_) => {
                if !matches!(val, Value::Null) {
                    if let Aggregator::Count(c) = self {
                        *c += 1;
                    }
                }
            }
            Aggregator::Sum(current) => {
                if !matches!(val, Value::Null) {
                    if matches!(current, Value::Null) {
                        *current = val.clone();
                    } else {
                        *current = add_values(current, val)?;
                    }
                }
            }
            Aggregator::Max(current) => {
                if !matches!(val, Value::Null) {
                    if matches!(current, Value::Null) {
                        *current = val.clone();
                    } else {
                        if compare_values(val, current)? > 0 {
                            *current = val.clone();
                        }
                    }
                }
            }
            Aggregator::Min(current) => {
                if !matches!(val, Value::Null) {
                    if matches!(current, Value::Null) {
                        *current = val.clone();
                    } else {
                        if compare_values(val, current)? < 0 {
                            *current = val.clone();
                        }
                    }
                }
            }
            Aggregator::Avg { sum, count } => {
                if !matches!(val, Value::Null) {
                    let v = match val {
                        Value::Int32(i) => *i as f64,
                        Value::Int64(i) => *i as f64,
                        Value::Float64(f) => *f,
                        _ => return Err(anyhow!("AVG requires numeric type")),
                    };
                    *sum += v;
                    *count += 1;
                }
            }
        }
        Ok(())
    }

    pub fn result(&self) -> Value {
        match self {
            Aggregator::Count(c) => Value::Int64(*c),
            Aggregator::Sum(v) => v.clone(),
            Aggregator::Max(v) => v.clone(),
            Aggregator::Min(v) => v.clone(),
            Aggregator::Avg { sum, count } => {
                if *count == 0 {
                    Value::Null
                } else {
                    Value::Float64(*sum / *count as f64)
                }
            }
        }
    }
}

fn add_values(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l + r)),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l + r)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64(*l as i64 + r)),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(l + *r as i64)),
        (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(l + r)),
        (Value::Int32(l), Value::Float64(r)) => Ok(Value::Float64(*l as f64 + r)),
        (Value::Float64(l), Value::Int32(r)) => Ok(Value::Float64(l + *r as f64)),
        _ => Err(anyhow!("Unsupported types for SUM")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count() {
        let mut agg = Aggregator::new("COUNT").unwrap();
        agg.update(&Value::Int32(1)).unwrap();
        agg.update(&Value::Int32(2)).unwrap();
        agg.update(&Value::Null).unwrap();
        agg.update(&Value::Int32(3)).unwrap();
        assert_eq!(agg.result(), Value::Int64(3));
    }

    #[test]
    fn test_count_empty() {
        let agg = Aggregator::new("COUNT").unwrap();
        assert_eq!(agg.result(), Value::Int64(0));
    }

    #[test]
    fn test_sum_int32() {
        let mut agg = Aggregator::new("SUM").unwrap();
        agg.update(&Value::Int32(10)).unwrap();
        agg.update(&Value::Int32(20)).unwrap();
        agg.update(&Value::Int32(30)).unwrap();
        assert_eq!(agg.result(), Value::Int32(60));
    }

    #[test]
    fn test_sum_with_null() {
        let mut agg = Aggregator::new("SUM").unwrap();
        agg.update(&Value::Int32(10)).unwrap();
        agg.update(&Value::Null).unwrap();
        agg.update(&Value::Int32(20)).unwrap();
        assert_eq!(agg.result(), Value::Int32(30));
    }

    #[test]
    fn test_sum_empty() {
        let agg = Aggregator::new("SUM").unwrap();
        assert_eq!(agg.result(), Value::Null);
    }

    #[test]
    fn test_max() {
        let mut agg = Aggregator::new("MAX").unwrap();
        agg.update(&Value::Int32(5)).unwrap();
        agg.update(&Value::Int32(10)).unwrap();
        agg.update(&Value::Int32(3)).unwrap();
        assert_eq!(agg.result(), Value::Int32(10));
    }

    #[test]
    fn test_max_with_null() {
        let mut agg = Aggregator::new("MAX").unwrap();
        agg.update(&Value::Null).unwrap();
        agg.update(&Value::Int32(5)).unwrap();
        agg.update(&Value::Null).unwrap();
        assert_eq!(agg.result(), Value::Int32(5));
    }

    #[test]
    fn test_min() {
        let mut agg = Aggregator::new("MIN").unwrap();
        agg.update(&Value::Int32(5)).unwrap();
        agg.update(&Value::Int32(2)).unwrap();
        agg.update(&Value::Int32(8)).unwrap();
        assert_eq!(agg.result(), Value::Int32(2));
    }

    #[test]
    fn test_avg() {
        let mut agg = Aggregator::new("AVG").unwrap();
        agg.update(&Value::Int32(10)).unwrap();
        agg.update(&Value::Int32(20)).unwrap();
        agg.update(&Value::Int32(30)).unwrap();
        let result = agg.result();
        assert!(matches!(result, Value::Float64(f) if (f - 20.0).abs() < 0.001));
    }

    #[test]
    fn test_avg_with_null() {
        let mut agg = Aggregator::new("AVG").unwrap();
        agg.update(&Value::Int32(10)).unwrap();
        agg.update(&Value::Null).unwrap();
        agg.update(&Value::Int32(20)).unwrap();
        let result = agg.result();
        assert!(matches!(result, Value::Float64(f) if (f - 15.0).abs() < 0.001));
    }

    #[test]
    fn test_avg_empty() {
        let agg = Aggregator::new("AVG").unwrap();
        assert_eq!(agg.result(), Value::Null);
    }

    #[test]
    fn test_avg_float() {
        let mut agg = Aggregator::new("AVG").unwrap();
        agg.update(&Value::Float64(1.5)).unwrap();
        agg.update(&Value::Float64(2.5)).unwrap();
        let result = agg.result();
        assert!(matches!(result, Value::Float64(f) if (f - 2.0).abs() < 0.001));
    }

    #[test]
    fn test_unsupported_aggregator() {
        assert!(Aggregator::new("UNKNOWN").is_err());
    }

    #[test]
    fn test_max_text() {
        let mut agg = Aggregator::new("MAX").unwrap();
        agg.update(&Value::Text("apple".to_string())).unwrap();
        agg.update(&Value::Text("banana".to_string())).unwrap();
        agg.update(&Value::Text("cherry".to_string())).unwrap();
        assert_eq!(agg.result(), Value::Text("cherry".to_string()));
    }

    #[test]
    fn test_min_text() {
        let mut agg = Aggregator::new("MIN").unwrap();
        agg.update(&Value::Text("banana".to_string())).unwrap();
        agg.update(&Value::Text("apple".to_string())).unwrap();
        agg.update(&Value::Text("cherry".to_string())).unwrap();
        assert_eq!(agg.result(), Value::Text("apple".to_string()));
    }
}
