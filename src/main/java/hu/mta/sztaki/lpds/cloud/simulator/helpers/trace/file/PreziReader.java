package hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file;

import java.lang.reflect.InvocationTargetException;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;

public class PreziReader extends TraceFileReaderFoundation {

	public PreziReader(String fileName, int from, int to, boolean allowReadingFurther, Class<? extends Job> jobType)
			throws SecurityException, NoSuchMethodException {
		super("LOG format", fileName, from, to, allowReadingFurther, jobType);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Parses a single line of the trace and creates a Prezi Job object out of it.
	 * 
	 * @param line the trace-line to be parsed
	 * @return a job object that is equivalent to the trace-line specified in the
	 *         input
	 * @throws IllegalArgumentException  error using the constructor of the job
	 *                                   object
	 * @throws InstantiationException    error using the constructor of the job
	 *                                   object
	 * @throws IllegalAccessException    error using the constructor of the job
	 *                                   object
	 * @throws InvocationTargetException error using the constructor of the job
	 *                                   object
	 */
	protected Job createJobFromLine(String line)
			throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		String[] lineData = line.split(" ");

		try {
			// Data ordered in same order as jobCreator arguments
			String id = lineData[2].strip(); // Job ID, removing any whitespace.
			long submit = Long.parseLong(lineData[0]); // Job arrival time in milliseconds
			long queue = 0; // Assume the wait time is 0
			long exec = (long) Float.parseFloat(lineData[1]); // Jobs duration in seconds
			int nprocs = 1; // Assume allocated processors is 1
			double ppCpu = -1; // If this is less than 0, automatically assign
			long ppMem = 512; // Assume average memory is 512
			String user = null; // Assume user is null
			String group = null; // Assume group is null
			String executable = lineData[3]; // Job executable type
			Job preceding = null; // Assume preceding job is null.
			long delayAfter = 0; // No delay.

			return jobCreator.newInstance(id, submit, queue, exec, nprocs, ppCpu, ppMem, user, group, executable,
					preceding, delayAfter);
		} catch (ArrayIndexOutOfBoundsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	protected void metaDataCollector(String line) {
		// TODO Auto-generated method stub
	}

	/**
	 * Determines if "line" can be considered as something that can be used to
	 * instantiate a job object; A trace-line is considered useful if it conforms to
	 * the following structure:
	 * <ul>
	 * <li>Job Arrival Time in Integer/UNIX time ( in milliseconds )</li>
	 * <li>Job Duration Float ( in seconds )</li>
	 * <li>Job ID String ( No Whitespace )</li>
	 * <li>Job Executable Name String ("url", "default" or "export")</li>
	 * </ul>
	 * 
	 * @param line the line in question
	 * @return true if "line" is a useful job descriptor.
	 */
	protected boolean isTraceLine(String line) throws ArrayIndexOutOfBoundsException {
		
		String[] lineData; // Instantiate string array.
		
		// Check if the line argument is null.
		if(line != null) {
			// Not null, add its contents to the array.
			lineData = line.split(" ");
		} else {
			// Line is null, invalid.
			return false;
		}
		
		// Catch invalid lines with ArrayIndexOutOfBoundsException
		try {
			// Check for Job Arrival Time Integer
			try {
				Integer.parseInt(lineData[0]);
			} catch (NumberFormatException e) {
				// Data can't be parsed as an integer, data line contains incorrect data.
				return false;
			}
			
			// Check for Job Duration Float
			try {
				Float.parseFloat(lineData[1]);
			} catch (NumberFormatException e) {
				// Data can't be parsed as a float, data line contains incorrect data.
				return false;
			}
			
			// Check for Job ID String ( Not Whitespace )
			if(lineData[2].isBlank()) {
				// Is made up of only whitespace.
				return false;
			}
			
			// Check for Job Executable String ( "url", "default" or "export" )
			if(!lineData[3].isBlank()) {
				// Not blank
				if(!lineData[3].equals("url") && !lineData[3].equals("default") && !lineData[3].equals("export")) {
					// Doesn't equal any of the specified job executable types
					return false;
				}
			} else {
				// Is made up of only whitespace.
				return false;
			}
			
			// Passed all of the checks, return true.
			return true;
			
		} catch(ArrayIndexOutOfBoundsException e) {
			// Incorrect amount of arguments.
			return false;
		}
		
	}
}
