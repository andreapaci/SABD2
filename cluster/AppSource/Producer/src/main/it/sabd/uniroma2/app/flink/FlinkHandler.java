package it.sabd.uniroma2.app.flink;

import it.sabd.uniroma2.app.util.Constants;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.configuration.Configuration;
import java.io.File;


public class FlinkHandler {

    private Configuration configuration;

    public FlinkHandler(){

        configuration = new Configuration();
        configuration.setString(JobManagerOptions.ADDRESS, Constants.FLINK_HOSTNAME);
        configuration.setInteger(RestOptions.PORT, Constants.FLINK_PORT);
        configuration.setInteger(RestOptions.RETRY_MAX_ATTEMPTS, 3);


    }



    public void submitJob(){

        RestClusterClient<StandaloneClusterId> flinkClient = null;
        PackagedProgram program = null;

        try {
            flinkClient = new RestClusterClient<>(configuration, StandaloneClusterId.getInstance());
            System.out.println("Cluster ID: " + flinkClient.getClusterId().toString());
            System.out.println("Cluster URL: " + flinkClient.getWebInterfaceURL());
        } catch (Exception e) {
            System.out.println("Error connecting to FLINK REST API.\nExiting...");
            e.printStackTrace();
            System.exit(-1);
        }
        String[] args = new String[]{"scan"};

        try {
            program = PackagedProgram.newBuilder()
                    .setJarFile(new File(Constants.APP_PATH))
                    .setArguments(args)
                    .build();

            System.out.println("Program Main Class: " + program.getMainClassName());
            System.out.println("Program Path: " + program.getJobJarAndDependencies().get(0).getPath());

        } catch (Exception e) {
            System.out.println("Could not build jar program.\nExiting");
            e.printStackTrace();
            System.exit(-1);
        }


        try {
            JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, configuration, 1, false);

            System.out.println("Job Graph vertexes: " + jobGraph.getNumberOfVertices());
            System.out.println("Job Graph Path: " + jobGraph.getUserJars().get(0).getPath());

            JobID jobId = flinkClient.submitJob(jobGraph).get();

            System.out.println("Job ID: " + jobId);


            System.out.println("Job submitted to Flink successfully.");
        } catch (Exception e){
            System.out.println("Could not create or submit \"" + Constants.APP_PATH + "\" to Flink");
            e.printStackTrace();
        }
    }

}
