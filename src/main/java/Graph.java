import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public short tag;                 // 0 for a graph vertex, 1 for a group number
    public long group;                // the group where this vertex belongs to
    public long VID;                  // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
    /* ... */
}

public class Graph {

    /* ... */

    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("MyJob");
        /* ... First Map-Reduce job to read the graph */
        job.waitForCompletion(true);
        for ( short i = 0; i < 5; i++ ) {
            job = Job.getInstance();
            /* ... Second Map-Reduce job to propagate the group number */
            job.waitForCompletion(true);
        }
        job = Job.getInstance();
        /* ... Final Map-Reduce job to calculate the connected component sizes */
        job.waitForCompletion(true);
    }
}
