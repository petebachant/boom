# Benchmarks for boom
### Usage
#### Run
To run a benchmark\
`cargo bench <benchmark_name> [-- ARGS]`

#### Example
The following command runs the filter_benchmark bench with 2 and 200 passed as command line arguments. In this case, the 2 refers to the filter id used in the benchmark and 200 is the number of times the filter will run on the alerts.\
`cargo bench filter_benchmark -- 2 200`

#### filtering benchmark
[WIP]
As of right now, to benchmark the filter_worker, ensure there are appropriate candids inside of the "filterafterml" redis queue by running the previous stages of the pipeline to feed in candids to redis, then run the benchmark:

`cargo bench filter_benchmark -- <filter_id> <num_iterations>`

