# Testing Windowing with TextIO

#### Build and Run 
Run pipeline without lateness in the window
```
mvn clean install
java -cp target/windowing-textio-bundled-1.0-SNAPSHOT.jar org.kby.PipelineWithTextIo
```

#### How does it work?
Pipeline generates data using a sequence.<br />
Pipeline adds a little delay, groups (using a distinct) the data entries and writes them in files.<br />
Files are stored in local folder `windowing-textio/pipe_with_lateness_0s/emitted-files`. [See here](https://github.com/kiuby88/windowing-textio/blob/master/src/main/java/org/kby/PipelineWithTextIo.java#L71-L77)

Emitted file names are grouped using a distinct (distinct is used because allows to apply a window easily), please [see here](https://github.com/kiuby88/windowing-textio/blob/master/src/main/java/org/kby/PipelineWithTextIo.java#L78-L83). 

Then, if you check the log in the terminal, you will see several `WindowTracing` traces saying that file names are dropped as late in the distinct (`WindowAfterEmitted`). For example:
```
DEBUG org.apache.beam.sdk.util.WindowTracing - LateDataFilter: Dropping
element at 2020-05-12T14:05:14.999Z for
key:...windowing-textio/pipe_with_lateness_0s/emitted-files/[2020-05-12T14:05:10.000Z..2020-05-12T14:05:15.000Z)-ON_TIME-0-of-1.txt;
window:[2020-05-12T14:05:10.000Z..2020-05-12T14:05:15.000Z) since too far
behind inputWatermark:2020-05-12T14:05:19.799Z;
outputWatermark:2020-05-12T14:05:19.799Z`
```

#### Improved results
Maybe, finding log traces to check that data are missing is not the easyest way. Then, I added a second `TypeWriter` to write the file names as other files in the folder `files-after-distinct`. <br />
Files names in `emitted-files` and `files-after-distinct` **should be the same**. But in following picture you can see they are not, because files are discarded:

![image](https://i.ibb.co/RQd78yS/dataloss.png)




#### Run with lateness
Pass an lateness values in seconds using an arg. For example, to use 60 seconds
```
java -cp target/windowing-textio-bundled-1.0-SNAPSHOT.jar org.kby.PipelineWithTextIo 60
```

You can see file names are not discarded.
