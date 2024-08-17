# zkv
 Dead simple key-value database written in pure rust for fun

# Overview
This database is splitted into 3 main components :
- Bucket
- IO
- Controller

## Bucket
This database is designed with "Bucket"-like implementation of storing data. A bucket is simply a data structure that stores and holds data independently of each other. This means you can have a lot of bucket with each with an  independent storage. 

## IO

## Controller
The Controller acts as the bucket manager, it controls when, and how the bucket lives. it does not however, invovled in any way when transferring data from the bucket to the client, the IO thread for each connection has a direct acces
