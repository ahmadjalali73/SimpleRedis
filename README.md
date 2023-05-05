# SimpleRedis
it's a simple Redis database with c++

this project's purpose is for training C++
this is based on "build your own Redis with c\c++" book. the link to this book is this:
https://build-your-own.org/

I'm using codes of this book. there are only some minor changes in codes. but the real base is on this book.

### things that have been added to code by me
1. config file: you can change IP address, port number and number of threads in server. we use a json file as config file in /etc/redis path. a sample config file is in directory of server. you should only copy it in /etc/redis/config.json.

## Compile by source
the server only has one pre-request for build and it is rapid-json. rapidjson is a famous library in c++ for parsing json files.

to install it on debian and ubuntu, you should run the following command
```
sudo apt install rapidjson-dev
```

 to compile server or client, you should go to the respective directory and run the following commands
 ```
 mkdir build
 cd build
 cmake ..
 make
 ```

cuation: I'm still writing these codes. hence, maybe there are some times that my code has some problems.
