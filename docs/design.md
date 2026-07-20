## Lab-2: Chandy Lamport Algorithm in Action.
### High Level System Overview.
The system performs word counting while being resilient to failures (except for the coordinator). It has 3 components:- 1) Mapper, 2) Reducer and 3) Coordinator.
The resiliency is achieved by using checkpoints. In this method, every worker(mapper & reducer) creates Checkpoints every once in a while.
And whenever a failure happens, all of them rollback to the last Checkpoint. However, the Checkpoints across the workers needs to be consistent and to achieve that we use Chandy Lamport's Algorithm.

#### The Mapper
##### Main Tasks:-
1) Mapper receives file names from a redis stream.
2) Reads words from the file and based on a mapping(word -> reducer_id), sends certain words to certain reducer.

##### Auxilary Tasks:-
1) Checkpoints itself when asked by the coordinator.
2) Recovers itself when asked by the coordinator.
3) Forward Checkpoint Markers to the reducer.
4) Send HeartBeats to the Coordinator.
5) Notify Coordinator when no more files are left to process.

#### The Reducer

##### Main Task:-
1) Receives the words from the mappers, and keep a counter for each word.

##### Auxilary Task:-
1) Checkpoints itself when asked by the coordinator.
2) Recovers itself when asked by the coordinator.
3) Send HeartBeats to the Coordinator. 

#### The Coordinator
##### Main Tasks:-
1) Send Checkpoint Markers.
2) Monitor if anyone misses HeartBeats.
3) Send Recovery Command when required.
4) Send Exit Command when required.


In this assignment we fix the number of mappers and reducers to be 2 each accompanied by a single coordinator.
So the system looks like the following. ![HighLevelDesign](https://github.com/codenet/col733/blob/recovery4/COL733_2024/lab2_chandy_lamport/lab2_1.png?raw=true)

### Low Level System Design

#### Mapper Design Pattern.
1. A mapper is a Python process.
2. This process has 4 threads. 
   1. HeartBeats Thread:- Sends HeartBeats to the coordinator over a UDP connection.
   2. WordCount Thread:- Receives files from the redis stream, and puts a WordCount command in a queue (Queue is explained later). If there are no files to process, Done command is added to the queue instead.
   3. Coordinator Thread:- Receives Checkpoint/Recovery/Exit messages from the coordinator and puts command in a queue. 
   4. Command Handler Thread:- Reads from the command queue and executes the command. There can be 4 such commands:- WordCount, Done, Checkpoint, Recover, Exit.
3. The process simply starts and waits for these threads to be completed. The queue is used as a synchronization primitive between the threads. Since there is only one consumer of the queue (Command Handler), there is no race.

##### Messages that can be received:-
1. Filename from RedisStream
2. Checkpoint Marker from Coordinator
3. Recovery Message from Coordinator
4. Exit Message from Coordinator

##### Messages that can be sent:-
1. Checkpoint marker to the reducers
2. HeartBeats to the coordinator
3. Checkpoint Ack to the coordinator
4. Recovery Ack to the coordinator
5. Done message to the coordinator
6. Exit Ack to the coordinator

#### Reducer Design Pattern.
1. A reducer is a Python process.
2. This process has 4 threads.
   1. HeartBeats Thread:- Sends HeartBeats to the coordinator over a UDP connection.
   2. MapperHandler-1 Thread:- Receives words and Checkpoint markers from the mapper-1, and puts the command in a queue.
   3. MapperHandler-2 Thread:- Receives words and Checkpoint markers from the mapper-2, and puts the command in a queue.
   4. Coordinator Thread:- Receives Recovery/Exit messages from the coordinator and puts these commands in a queue. 
   5. Command Handler Thread:- Reads from the command queue and executes the command. There can be 4 such commands:- WordCount, Checkpoint, Recover, Exit.
3. The process simply starts and waits for these threads to be completed. Note that the Checkpoint marker is received from the mapper not the coordinator. 

##### Messages that can be received:-
1. Words from Mappers
2. Checkpoint Marker from Mappers
3. Recovery Message from Coordinator
4. Exit Message from Coordinator

##### Messages that can be sent:-
1. HeartBeats to the coordinator
2. Checkpoint Ack to the coordinator
3. Recovery Ack to the coordinator
4. Exit Ack to the coordinator

#### Coordinator Design Pattern.
1. A coordinator is a Python process.
2. This process has 2 threads.
   1. Sender Thread (Stateless) :- Sends messages to the mapper/reducers based on the current PHASE.
   2. Receiver Thread:- Receives messages from the mapper/reducer and changes the current PHASE (if required).
3. The process simply starts and waits for these threads to be completed.

##### Messages that can be received:-
1. HeartBeatss from Mappers/Reducers
2. Checkpoint Ack from Mappers/Reducers
3. Recovery Ack from Mapper/Recuer
4. Exit Ack from Mapper/Reducer

##### Messages that can be sent:-
1. Checkpoint to the M/R
2. Recovery message M/R
3. Exit message M/R

### Communication
Communication between mapper -> reducer happens via a persistent TCP connection.
All communication between coordinator <-> mapper/reducer happens on an UDP connection. 

Summary is as follows:- ![HighLevelDesign](https://github.com/codenet/col733/blob/recovery4/COL733_2024/lab2_chandy_lamport/lab2_2.png?raw=true)


#### Assumptions
1. Recovery is always successful (wont crash while recovery.)
2. Even if the UPD is unreliable, we assume that the message is delivered without corruption.
