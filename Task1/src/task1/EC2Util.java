package task1;

import java.util.List;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DeleteTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

public class EC2Util {
	private PropertiesCredentials Credentials;
	private AmazonEC2 ec2;
	private static final String instancesName = "ami-146e2a7c";
	private static final String instancesType = InstanceType.T1Micro.toString();
	
	public EC2Util(PropertiesCredentials creds) {
		Credentials = creds;
		ec2 = new AmazonEC2Client(Credentials);
	}
	
	public List<Instance> createNode(int numberOfMachines) {
		RunInstancesRequest request = new RunInstancesRequest(instancesName, numberOfMachines, numberOfMachines);
		request.setInstanceType(instancesType);
		List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
		return instances;
	}
	
	public Instance getManagerInstance() {
		DescribeInstancesResult result = ec2.describeInstances();
		List<Reservation> reservations = result.getReservations();

		for (Reservation reservation : reservations) {
			List<Instance> instances = reservation.getInstances();
			
			for (Instance instance : instances) {
				if ( instance.getTags().contains(new Tag("Manager", "True")) ) {
					return instance;
		    	  }	
		      }
		 }
		return null;
	}

	public void setManagerTag(Instance remoteManagerInstance) {
		CreateTagsRequest createTagsRequest=new CreateTagsRequest().withResources(remoteManagerInstance.getInstanceId()).withTags(new Tag("Manager","True"));
		ec2.createTags(createTagsRequest);
	}

	public void removeManagerTag(Instance remoteManagerInstance) {
		DeleteTagsRequest createTagsRequest=new DeleteTagsRequest().withResources(remoteManagerInstance.getInstanceId()).withTags(new Tag("Manager","True"));
		ec2.deleteTags(createTagsRequest);
	}

	public void terminateMachine(Instance remoteManagerInstance) {
		
//    	TerminateInstancesRequest request = new TerminateInstancesRequest();
//    	request.withInstanceIds(remoteManagerInstance.getInstanceId());
//    	ec2.terminateInstances(request);
	}
	
}
