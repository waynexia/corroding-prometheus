Native implementation of PromQL
-------------------------------

## Design In Detail

### Data Model

Four basic types:
- Scalar
- String
- Instant Vector
- Range Vector

To maximize the performance, I propose to perform calculation in columnar, rather than by timestamp like PromQL. Which means both instant vector and range vector cannot be transplanted as-is.

### Data Preparation

Corresponding to PromQL's `populateSeries` procedure. This step will be translated into two or three logical plan nodes
- `TableScan`: Read the data from table. This is a standard table scan operation.
- `SeriesNormalize`
- `Filter`: To ensure the result of is correct, an extra `Filter` node may also be required if the table implement cannot perform "exact" filter.

This step reads data from the underlying table. And to simplify this procedure, assumes the returned data is already organized (grouped) in time series format.

`SeriesNormalize` is a "extension plan" proposed in this RFC. It handles the logics like offset, look back, alignment, etc in PromQL. And more to support the following columnar evaluation.


TODO: detail this section

### Evaluation