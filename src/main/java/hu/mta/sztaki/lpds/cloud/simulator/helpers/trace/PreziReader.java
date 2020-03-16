package hu.mta.sztaki.lpds.cloud.simulator.helpers.trace;

import java.lang.reflect.InvocationTargetException;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file.TraceFileReaderFoundation;

public class PreziReader extends TraceFileReaderFoundation {

	protected PreziReader(String fileName, int from, int to, boolean allowReadingFurther, Class<? extends Job> jobType)
			throws SecurityException, NoSuchMethodException {
		super("LOG format", fileName, from, to, allowReadingFurther, jobType);
		// TODO Auto-generated constructor stub
	}

	protected boolean isTraceLine(String line) {

		String[] lineArray = line.split(" ");

		try {
			Integer.parseInt(lineArray[0]); // Check if integer
			Float.parseFloat(lineArray[1]); 
		} catch (Exception e) {
			return false;
		}
	
		if(lineArray[2].equals(null)){
			return false;
		}
		
		if(lineArray[3].equals(null)){
			return false;
		}
		
		return true;
	}



	protected Job createJobFromLine(String line)
			throws IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
		
		
		try{
			String[] lineArr = line.split(" ");
			long submit = Integer.parseInt(lineArr[0]);
			long ppMem = 512;
			float jobDuration = Float.parseFloat(lineArr[1]);
			
			String Id = lineArr[2];
			String executable = lineArr[3];
			String user = null;
			String group = null;
			String  preceding = null;
			
			long waitTime = 0;
			long delayAfter = 0;
			
			long queuetimeSecs  = 1;
			
			int nprocs = 1;
			return jobCreator.newInstance(Id, submit , queuetimeSecs, 0, nprocs, -1, ppMem, user, group, executable, preceding, delayAfter);
		} catch(ArrayIndexOutOfBoundsException ex) {
			// Incomplete line, ignore it
			return null;
		}
	}

	@Override
	protected void metaDataCollector(String line) {
		// TODO Auto-generated method stub
		
	}
}
