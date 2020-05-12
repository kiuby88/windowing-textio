# Testing Windowing with TextIO

#### Build
`mvn clean install`

#### Run
Run pipeline without lateness in the window
`java -cp target/windowing-textio-bundled-1.0-SNAPSHOT.jar org.kby.PipelineWithTextIo`

#### Run with lateness
Pass an lateness values in seconds using an arg. For example, to use 60 seconds
`java -cp target/windowing-textio-bundled-1.0-SNAPSHOT.jar org.kby.PipelineWithTextIo 60`
