package org.apache.spark

import java.rmi.{Naming, Remote}
import java.rmi.registry._

import scala.reflect.{ClassTag, classTag}

import org.apache.spark.rdd.RDD

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

trait RemoteContextInterface extends Remote {
  def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int): RDD[T]
}

class RemoteContext(config: SparkConf) extends SparkContext(config) with RemoteContextInterface
