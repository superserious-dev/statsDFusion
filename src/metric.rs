use anyhow::Result;
use chumsky::{
    IterParser, Parser,
    error::Rich,
    extra,
    prelude::{any, choice, just, one_of},
    span::SimpleSpan,
    text,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::{collections::BTreeSet, fmt::Debug, ops::Div, sync::Arc};

#[derive(Clone, Debug, PartialEq)]
pub enum MetricValue<V: Debug + PartialEq> {
    Constant(V),
    Delta(V),
}

pub type Tags = BTreeSet<(String, Option<String>)>;

#[derive(Clone, Debug, PartialEq)]
pub enum Metric {
    CounterMetric {
        name: String,
        value: MetricValue<i64>,
        tags: Tags,
    },
    GaugeMetric {
        name: String,
        value: MetricValue<f64>,
        tags: Tags,
    },
}

impl Metric {
    pub fn parse(input: &str) -> Result<Metric, Vec<Rich<'_, char>>> {
        Self::parser().parse(input).into_result()
    }

    fn parser<'src>()
    -> impl Parser<'src, &'src str, Metric, extra::Err<Rich<'src, char, SimpleSpan>>> + Clone {
        // ***
        // Helpers
        // ***

        let fixed_integer = text::digits(10)
            .to_slice()
            .map(|s: &str| -> i64 { s.parse().unwrap() })
            .labelled("fixed integer");

        let fixed_float = text::int(10)
            .then(just('.').then(text::digits(10)))
            .to_slice()
            .map(|v: &str| -> f64 { v.parse().unwrap() })
            .labelled("fixed float");

        let delta_integer = one_of("+-")
            .then(text::digits(10))
            .to_slice()
            .map(|s: &str| -> i64 { s.parse().unwrap() })
            .labelled("delta integer");

        let delta_float = one_of("+-")
            .then(text::int(10).then(just('.').then(text::digits(10))))
            .to_slice()
            .map(|v: &str| -> f64 { v.parse().unwrap() })
            .labelled("delta float");

        // ***
        // Metric components
        // ***

        let name = any()
            .filter(|c: &char| c.is_alphanumeric() || ['.', '-', '_'].contains(c))
            .repeated()
            .to_slice()
            .filter(|n: &&str| !n.is_empty())
            .map(|v: &str| v.to_string())
            .labelled("name");

        let counter_specifier = just("|c").labelled("counter specifier");

        let gauge_specifier = just("|g").labelled("gauge specifier");

        let sample_rate = just("|@")
            .ignore_then(fixed_float)
            .filter(|v| *v >= 0. && *v <= 1.)
            .labelled("sample rate");

        let tags = just("|#")
            .ignore_then(
                text::unicode::ident()
                    .then(just(':').ignore_then(text::unicode::ident()).or_not())
                    .map(|(k, v): (&str, Option<&str>)| (k.to_string(), v.map(|s| s.to_string())))
                    .separated_by(just(','))
                    .collect(),
            )
            .labelled("tags");

        // ***
        // Metrics
        // ***

        choice((
            // Counter metric
            name.then(
                just(':').ignore_then(
                    fixed_integer
                        .map(MetricValue::Constant)
                        .or(delta_integer.map(MetricValue::Delta))
                        .then(
                            counter_specifier.ignore_then(sample_rate.or_not().then(tags.or_not())),
                        ),
                ),
            )
            .map(|(name, (value, (sample_rate, tags)))| {
                Metric::new_counter(name, value, sample_rate, tags)
            }),
            // Gauge metric
            name.then(
                just(':').ignore_then(
                    (fixed_float
                        .or(fixed_integer.map(|v| v as f64))
                        .map(MetricValue::Constant))
                    .or(delta_float
                        .or(delta_integer.map(|v| v as f64))
                        .map(MetricValue::Delta))
                    .then(gauge_specifier.ignore_then(tags.or_not())),
                ),
            )
            .map(|(name, (value, tags))| Metric::new_gauge(name, value, tags)),
        ))
    }

    fn new_counter(
        name: String,
        value: MetricValue<i64>,
        sample_rate: Option<f64>,
        tags: Option<Tags>,
    ) -> Self {
        let sampler = |value| (value as f64).div(sample_rate.unwrap_or(1f64)).floor() as i64;
        let sampled_value = match value {
            MetricValue::Constant(v) => MetricValue::Constant(sampler(v)),
            MetricValue::Delta(v) => MetricValue::Delta(sampler(v)),
        };

        Metric::CounterMetric {
            name,
            value: sampled_value,
            tags: tags.unwrap_or_default(),
        }
    }

    fn new_gauge(name: String, value: MetricValue<f64>, tags: Option<Tags>) -> Self {
        Metric::GaugeMetric {
            name,
            value,
            tags: tags.unwrap_or_default(),
        }
    }
}

/// Parses a packet of metrics into a vector of valid metrics
/// For now, only return metrics that were parsed successfully
pub fn parse_packet(packet: &str) -> Vec<Metric> {
    let mut metrics = vec![];
    for candidate in packet.lines() {
        let parsed = Metric::parse(candidate);
        if let Ok(parsed) = parsed {
            metrics.push(parsed);
        }
    }
    metrics
}

pub mod schema_fields {
    use super::*;

    pub fn name_field() -> Field {
        Field::new("name", DataType::Utf8, false)
    }

    pub fn tags_field() -> Field {
        Field::new_map(
            "tags",
            "entries",
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Utf8, true),
            false,
            false,
        )
    }

    pub fn flushed_at_field() -> Field {
        Field::new(
            "flushed_at",
            DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
            false,
        )
    }

    pub fn counter_value_field() -> Field {
        Field::new("value", DataType::Int64, false)
    }

    pub fn gauge_value_field() -> Field {
        Field::new("value", DataType::Float64, false)
    }

    pub fn counters_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            name_field(),
            counter_value_field(),
            tags_field(),
            flushed_at_field(),
        ]))
    }

    pub fn gauges_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            name_field(),
            gauge_value_field(),
            tags_field(),
            flushed_at_field(),
        ]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod counter_metric_tests {
        use super::*;

        #[test]
        fn test_name_constant_value_type() {
            assert_eq!(
                parse_packet("metric:1|c"),
                vec![Metric::CounterMetric {
                    name: "metric".to_string(),
                    tags: Tags::new(),
                    value: MetricValue::Constant(1)
                }]
            );

            assert_eq!(
                parse_packet("met_ric-0:1|c"),
                vec![Metric::CounterMetric {
                    name: "met_ric-0".to_string(),
                    tags: Tags::new(),
                    value: MetricValue::Constant(1)
                }]
            );
        }

        #[test]
        fn test_name_positive_delta_value_type() {
            assert_eq!(
                parse_packet("metric:+1|c"),
                vec![Metric::CounterMetric {
                    name: "metric".to_string(),
                    tags: Tags::new(),
                    value: MetricValue::Delta(1)
                }]
            );
        }
        #[test]
        fn test_name_negative_delta_value_type() {
            assert_eq!(
                parse_packet("metric:-1|c"),
                vec![Metric::CounterMetric {
                    name: "metric".to_string(),
                    tags: Tags::new(),
                    value: MetricValue::Delta(-1)
                }]
            );
        }

        #[test]
        fn test_name_value_type_sample_rate() {
            assert_eq!(
                parse_packet("metric:1|c|@0.5"),
                vec![Metric::CounterMetric {
                    name: "metric".to_string(),
                    tags: Tags::new(),
                    value: MetricValue::Constant(2)
                }]
            );
        }

        #[test]
        fn test_name_value_type_sample_rate_with_rounding() {
            assert_eq!(
                parse_packet("metric:17|c|@0.4"),
                vec![Metric::CounterMetric {
                    name: "metric".to_string(),
                    tags: Tags::new(),
                    value: MetricValue::Constant(42)
                }]
            );
        }

        #[test]
        fn test_name_value_type_sample_rate_tags() {
            assert_eq!(
                parse_packet("metric:1|c|@0.5|#tag1,tag2_key:tag2_value"),
                vec![Metric::CounterMetric {
                    name: "metric".to_string(),
                    tags: Tags::from([
                        ("tag1".to_owned(), None),
                        ("tag2_key".to_owned(), Some("tag2_value".to_owned()))
                    ]),
                    value: MetricValue::Constant(2)
                }]
            );
        }

        #[test]
        fn test_name_value_type_tags() {
            assert_eq!(
                parse_packet("metric:1|c|#tag1,tag2_key:tag2_value"),
                vec![Metric::CounterMetric {
                    name: "metric".to_string(),
                    tags: Tags::from([
                        ("tag1".to_owned(), None),
                        ("tag2_key".to_owned(), Some("tag2_value".to_owned()))
                    ]),
                    value: MetricValue::Constant(1)
                }]
            );
        }

        #[test]
        fn test_name_with_dot_value_type() {
            assert_eq!(
                parse_packet("a.metric:1|c"),
                vec![Metric::CounterMetric {
                    name: "a.metric".to_string(),
                    tags: Tags::new(),
                    value: MetricValue::Constant(1)
                }]
            );
        }

        #[test]
        fn test_invalid_name() {
            assert_eq!(parse_packet("met ric:1|c"), vec![]);
            assert_eq!(parse_packet("met|ric:1|c"), vec![]);
        }

        #[test]
        fn test_blank_name() {
            assert_eq!(parse_packet(":1|c"), vec![]);
        }

        #[test]
        fn test_invalid_type() {
            assert_eq!(parse_packet("metric:1|cc"), vec![]);
            assert_eq!(parse_packet("metric:1|z"), vec![]);
        }

        #[test]
        fn test_invalid_sample_rate_string() {
            assert_eq!(parse_packet("metric:1|c|@.0.1"), vec![]);
        }

        #[test]
        fn test_invalid_sample_rate_outside_range() {
            assert_eq!(parse_packet("metric:1|c|@-0.1"), vec![]);
            assert_eq!(parse_packet("metric:1|c|@1.1"), vec![]);
        }

        #[test]
        fn test_blank_sample_rate() {
            assert_eq!(parse_packet("metric:1|c|@"), vec![]);
        }

        #[test]
        fn test_invalid_tags() {
            assert_eq!(parse_packet("metric:1|c|#tag1|tag2"), vec![]);
        }
    }

    mod gauge_metric_tests {
        use super::*;

        #[test]
        fn test_name_constant_value_type() {
            assert_eq!(
                parse_packet("metric:1|g"),
                vec![Metric::GaugeMetric {
                    name: "metric".to_string(),
                    tags: Tags::new(),
                    value: MetricValue::Constant(1.)
                }]
            );

            assert_eq!(
                parse_packet("met_ric-0:1|g"),
                vec![Metric::GaugeMetric {
                    name: "met_ric-0".to_string(),
                    tags: Tags::new(),
                    value: MetricValue::Constant(1.)
                }]
            );
        }

        #[test]
        fn test_name_value_float_type() {
            assert_eq!(
                parse_packet("metric:1.2|g"),
                vec![Metric::GaugeMetric {
                    name: "metric".to_string(),
                    tags: Tags::new(),
                    value: MetricValue::Constant(1.2)
                }]
            );
        }

        #[test]
        fn test_name_positive_delta_value_type() {
            assert_eq!(
                parse_packet("metric:+1|g"),
                vec![Metric::GaugeMetric {
                    name: "metric".to_string(),
                    tags: Tags::new(),
                    value: MetricValue::Delta(1.)
                }]
            );
        }
        #[test]
        fn test_name_negative_delta_value_type() {
            assert_eq!(
                parse_packet("metric:-1|g"),
                vec![Metric::GaugeMetric {
                    name: "metric".to_string(),
                    tags: Tags::new(),
                    value: MetricValue::Delta(-1.)
                }]
            );
        }

        #[test]
        fn test_name_value_type_sample_rate() {
            assert_eq!(parse_packet("metric:1|g|@0.5"), vec![]);
        }

        #[test]
        fn test_name_value_type_tags() {
            assert_eq!(
                parse_packet("metric:1|g|#tag1,tag2_key:tag2_value"),
                vec![Metric::GaugeMetric {
                    name: "metric".to_string(),
                    tags: Tags::from([
                        ("tag1".to_owned(), None),
                        ("tag2_key".to_owned(), Some("tag2_value".to_owned()))
                    ]),
                    value: MetricValue::Constant(1.)
                }]
            );
        }

        #[test]
        fn test_name_with_dot_value_type() {
            assert_eq!(
                parse_packet("a.metric:1|g"),
                vec![Metric::GaugeMetric {
                    name: "a.metric".to_string(),
                    tags: Tags::new(),
                    value: MetricValue::Constant(1.)
                }]
            );
        }

        #[test]
        fn test_invalid_name() {
            assert_eq!(parse_packet("met ric:1|g"), vec![]);
            assert_eq!(parse_packet("met|ric:1|g"), vec![]);
        }

        #[test]
        fn test_blank_name() {
            assert_eq!(parse_packet(":1|g"), vec![]);
        }

        #[test]
        fn test_invalid_type() {
            assert_eq!(parse_packet("metric:1|gg"), vec![]);
            assert_eq!(parse_packet("metric:1|z"), vec![]);
        }

        #[test]
        fn test_invalid_tags() {
            assert_eq!(parse_packet("metric:1|g|#tag1|tag2"), vec![]);
        }
    }

    mod shared {
        use super::*;

        #[test]
        fn test_multiple_metrics() {
            assert_eq!(
                parse_packet("counter_metric:1|c\ngauge_metric:1.23|g"),
                vec![
                    Metric::CounterMetric {
                        name: "counter_metric".to_string(),
                        tags: Tags::new(),
                        value: MetricValue::Constant(1)
                    },
                    Metric::GaugeMetric {
                        name: "gauge_metric".to_string(),
                        tags: Tags::new(),
                        value: MetricValue::Constant(1.23)
                    },
                ]
            );
        }

        #[test]
        fn test_duplicate_tags() {
            assert_eq!(
                parse_packet("metric:1|g|#tag1,tag2_key:tag2_value,tag1,tag2_key:tag2_value"),
                vec![Metric::GaugeMetric {
                    name: "metric".to_string(),
                    tags: Tags::from([
                        ("tag1".to_owned(), None),
                        ("tag2_key".to_owned(), Some("tag2_value".to_owned()))
                    ]),
                    value: MetricValue::Constant(1.)
                }]
            );
        }

        #[test]
        fn test_blank_type() {
            assert_eq!(parse_packet("metric:1"), vec![]);
            assert_eq!(parse_packet("metric:1|"), vec![]);
        }
    }
}
