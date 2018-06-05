# MIT6.824-Lab

Naive implement of mit6.824 lab. Please don't use these code other than for reference.

## progress:
* lab 1 √
* lab 2 √

## design detail
### lab 1
Just implement what in the paper

### lab 2
Most part is directly derived from paper. Generally, i build it in event loop fashion.

And also some subtle details, e.g., the optimization mentioned from the bottom of page 7 to top of page 8, 
which is said unneccessary in real production environment, is proven critical to pass test `Figure8Unreliable2C`. 
So i figure it out with my own solution. You can found these details and my solution of them in the code.

What lacks: now, my implement can run 2B as fast as the given example on the lab's page. However, it takes 30s 
more to finish 2C more than the example does. I guess the read/write lock used in leader's work is the cause. I'll 
think about it later.
