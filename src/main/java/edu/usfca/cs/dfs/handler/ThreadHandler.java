package edu.usfca.cs.dfs.handler;

import edu.usfca.cs.dfs.StorageMessages;

import java.util.concurrent.Future;

public class ThreadHandler {

    public static Future<StorageMessages> getStorageNodeFromControllerAsynchronously(String fileName) {
        new Thread(new Runnable() {
            public void run()
            {

                // perform any operation
                System.out.println("Performing operation in Asynchronous Task");

                // check if storage node is added to the chunk mapping data structure
//                if (mListener != null) {

                    // notify the client to request chunk from storage node
//                    mListener.onGeekEvent();
//                }
            }
        }).start();
    }
}
