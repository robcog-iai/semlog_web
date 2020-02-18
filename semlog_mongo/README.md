# semlog_mongo
Semlog python interface connected with MongoDB

## Getting Started
Clone the repo and run on your local machine for development and testing purposes.

### Prerequisites
Python 3.7

### Installing

```
git clone https://github.com/robcog-iai/semlog_mongo.git
```


## Running the tests

- Initialize an instance of the class MongoDB. For example: 

 ```python
from semlog_mongo.semlog.mongo import MongoDB
db=MongoDB("SemLog","20")
```
- Query the database by:

 ```python
 result=db.search(object_id='hf-T8iy_c0CxIwAUOw7zrQ')
 ```
 - Download image by:
 ```python
 db.download(db.search(object_id='hf-T8iy_c0CxIwAUOw7zrQ'))
 ```
## Built with

* [python]("https://docs.mongodb.com/ecosystem/drivers/pymongo/") - The python interface for MongoDB


## Authors

* **Xiaoyue Zhang** - *Author* - [xinliy](https://github.com/xinliy)


## License

This project is licensed under the BSD-3-Clause License

## Acknowledgments

* This framework is developed by Xiaoyue Zhang in the Institute of Artificial Intelligence, University of Bremen, under the supervision of Andrei Haidu
