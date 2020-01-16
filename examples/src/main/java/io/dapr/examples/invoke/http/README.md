# Method invocation Sample

In this sample, we'll create a two java applications: An exposer service application which exposes a method and a client application which will invoke the method from demo service using Dapr.
This sample includes:

* ExposerService (An application that exposes a method to be invoked)
* InvokeClient (Invokes the exposed method from DemoService)

Visit [this](https://github.com/dapr/docs/blob/master/concepts/service-invocation/service-invocation.md) link for more information about Dapr and service invocation.
 
## Remote invocation using the Java-SDK

This sample uses the Client provided in Dapr Java SDK for exposing and invoking a remote method.  


## Pre-requisites

* [Dapr and Dapr Cli](https://github.com/dapr/docs/blob/master/getting-started/environment-setup.md#environment-setup).
* Java JDK 11 (or greater): [Oracle JDK](https://www.oracle.com/technetwork/java/javase/downloads/index.html#JDK11) or [OpenJDK](https://jdk.java.net/13/).
* [Apache Maven](https://maven.apache.org/install.html) version 3.x.

### Checking out the code

Clone this repository:

```sh
git clone https://github.com/dapr/java-sdk.git
cd java-sdk
```

Then build the Maven project:

```sh
# make sure you are in the `java-sdk` directory.
mvn install
```

### Running the Exposer service sample

The Exposer service application is a simple java class with a main method that uses the Dapr Client to declare and expose a method. Such method can now be invoked remotely. 

In the `ExposerService.java` file, you will find the `ExposerService` class, containing the main method. The main method declares the method to be invoked remotely. This method will recieve two parameters: `data` and `metadata` and will print them whenever the method is invoked. See the code snippet below:

```java
public class ExposerService {
///...
  public static void main(String[] args) throws Exception {
    ///...
    Dapr.getInstance().registerServiceMethod("say", (data, metadata) -> {
      String message = data == null ? "" : new String(data, StandardCharsets.UTF_8);

      Calendar utcNow = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
      String utcNowAsString = DATE_FORMAT.format(utcNow.getTime());

      String metadataString = metadata == null ? "" : OBJECT_MAPPER.writeValueAsString(metadata);

      // Handles the request by printing message.
      System.out.println(
        "Server: " + message + " @ " + utcNowAsString + " and metadata: " + metadataString);

      return Mono.just(utcNowAsString.getBytes(StandardCharsets.UTF_8));
    });
    //..
    }
///...
}
```

This sample registers a method called `say` using the static `Dapr.getInstance().registerServiceMethod` method. The method retreives the data and metadata and prints them along with the current date in console. The actual response from method is the formatted current date. 

Use the follow command to execute the exposer example:

```sh
dapr run --app-id invokedemo --app-port 3000 --port 3005 -- mvn exec:java -pl=examples -Dexec.mainClass=io.dapr.examples.invoke.http.DemoService -Dexec.args="-p 3000"
```

Once running, the ExposerService is now ready to be invoked by Dapr.


### Running the InvokeClient sample

The Invoke client sample uses the Dapr SDK for invoking the remote method. It needs to know the method name to invoke as well as the application id for the remote application. In `InvokeClient.java` file, you will find the `InvokeClient` class and the `main` method. See the code snippet below:

```java
public class InvokeClient {

private static final String SERVICE_APP_ID = "invokedemo";
///...
  public static void main(String[] args) {
    DaprClient client = (new DaprClientBuilder()).build();
    for (String message : args) {
      client.invokeService(Verb.POST, SERVICE_APP_ID, "say", message, null, String.class).block();
    }
  }
///...
}
```

The class knows the app id for the remote application. It uses the the static `Dapr.getInstance().invokeService` method to invoke the remote method defining the parameters: The verb, application id, method name, and proper data and metadata, as well as the type of the expected retun data.
 
 Execute the follow script in order to run the InvokeClient example, passing two messages for the remote method:
```sh
dapr run --port 3006 -- mvn exec:java -pl=examples -Dexec.mainClass=io.dapr.examples.invoke.http.InvokeClient -Dexec.args="'message one' 'message two'"
```
Once running, the outpust should read as follows:

![publisheroutput](../../../../../resources/img/subscriber.png)

Method have been remotely invoked.