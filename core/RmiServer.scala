package org.apache.spark

import java.rmi.Naming
import java.rmi.registry._

object RmiServer {

    def main(args: Array[String]) {
        System.out.println("RMI server started");

            LocateRegistry.createRegistry(1099);
            System.out.println("java RMI registry created.");

        //Instantiate RmiServer
        val conf = new SparkConf()
                    .setMaster("local")
                    .setAppName("My application")

        val sc: RemoteContextInterface = new RemoteContext(conf)

        // Bind this object instance to the name "RmiServer"
        Naming.rebind("//localhost/RmiServer", sc);
        System.out.println("PeerServer bound in registry");
    }
}

@remote
trait RemoteContextInterface extends java.rmi.Remote {
    def getLocalProperty(key: String): String
}

class RemoteContext(config: SparkConf) extends SparkContext(config) with RemoteContextInterface
