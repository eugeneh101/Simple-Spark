# Simple Spark

Have you ever wondered how Apache Spark and Beam work under the cover? Both Spark and Beam are high-performance, data-parallel processing frameworks generally run on a compute cluster. Basically, they are used for ETL to maximize throughput.  
<br>
Parallelism can occur at the **chip** level: Single instruction, multiple data (SIMD) instructions can utilize the full width of the registers on a CPU (NumPy) and GPU (TensorFlow).  
Parallelism can also occur at the **process** level using libraries such as `multiprocessing`, `ipyparallel`, and `MRJob`. You can take a look at my comparision between ipyparallel and MRJob: https://github.com/eugeneh101/ipyparallel-vs-MRJob  
Parallelism can also occur at the **machine** level where multiple machines work cooperatively in a cluster--that's what Spark and Beam do.  
<br>
I'm deeply curious about the implementation underlying Spark in which you chain a bunch of transformations to create a Directed Acyclic Graph (DAG). The graph is defined lazily, so the actual computation is deferred until an action is called. In this repo, I focus on building the transformation DAG (ie. how do you perform a `groupByKey()` in a lazy way?). The annotations below explain the pros and cons for each of my implementations, which are ordered from simplest to most sophisticated and complete. My notebook requires Python 3.3+. Enjoy!  

Disclaimer: This repo does not focus on the parallelized nature of computation across machines.
<br><br>


#### 1st Attempt
Pros: 
* simple generator style to create transforms
  
Cons: 
* only works for first action. Once called, no further actions can be performed

#### 2nd Attempt
Pros:
* 2nd implemention using generator style
* slight improvement in that calling an action the second time will return non-empty list. 

Cons: 
* still suffers from the same problem of only can perform (correct) action 1 time
* only the transformations up to the last transformation are applied--cannot get the correct result from action for first transformation if there exists a second transformation

#### 3rd Attempt
Pros:
* decorator style: composing higher order functions to emulate the sequence of transformations
* added `reduce()` action
* can create multiple RDDs where actions will get you the correct result
* decoupled the RDDs, hence RDDs are immutable

Cons:
* cannot do `filter()` or `groupByKey()` due to the inherent limitation of using decorators

#### 4th Attempt
Pros:
* instead of generator or decorator, implemented RDD transformations as a list of map functions

Cons:
* `reduceByKey()`, `filter()`, and `groupByKey()` not implemented

#### 5th Attempt
Pros:
* 2nd implementation using a list of functions and generator function
* `reduceByKey()` transformation implemented
* `filter()` transformation implemented weakly
* attempted to implement `flatMap()`

Cons:
* `reduceByKey()` uses `groupByKey()`, which creates a dictionary with keys and lists of values. Hence, memory usage can spike if the list of values corresponding to its key is very large
* `filter()` doesn't work if transformed element is falsy (False, 0, "")
* `flatMap()` doesn't work correctly due to limitation of how `__results_generator__()` iterates though elements in RDD

#### 6th Attempt (Best Implementation)
Pros:
* 3rd implementation using a list of functions (and recursive generator function)
* superior implementation of `reduceByKey()` where two values for a given key is immediately reduced into 1 value. Hence, each key will always have only 1 value
* superior implementation of `filter()` transformation using the recursive generator function `__results_generator__()`
* `flatMap()` implementation fixed using the recursive generator function `__results_generator__()`

Cons:
* can only reify (call an action on) an inputted generator 1 time--if you `parallelize()` a generator, then the second time you call an action, an empty list will be returned. Only a weakness for generators as they can only be iterated through once. If you `parallelize()` a list or tuple, then the second time you call an action, the correct answer will be returned.

#### 7th Attempt
Pros:
* 4th implementation using a  list of functions (and recursive generator function)
* even if you `parallelize()` a generator, the second time you call an action, RDD will return the correct result. After the first reify, the generator (that you `parallelize()`) is memoized as a list
* Moved the type checking (whether if the current RDD's `__sequence__` is a (generator) function, generator, or container) from `__results_generator__()` to `__sequence_generator__()`

Cons:
* despite being more complete than the 6th implementation, it is harder to read