# Benchmarks for boom
## Usage
### Run
To run a benchmark\
`cargo bench <benchmark_name> [-- ARGS]`

### Benchmarks

#### filtering benchmark

`cargo bench filter_benchmark -- <filter_id> <num_iterations>`

 - **Example**:\
The following command runs the filter_benchmark bench with 2 and 200 passed as command line arguments. In this case, the 2 refers to the id of the filter in mongodb that will be used in the benchmark and 200 is the number of times the filter will run on the alerts.\
`cargo bench filter_benchmark -- 2 200`
