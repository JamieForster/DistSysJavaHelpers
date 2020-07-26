/*
 *  ========================================================================
 *  Helper classes to support simulations of large scale distributed systems
 *  ========================================================================
 *  
 *  This file is part of DistSysJavaHelpers.
 *  
 *    DistSysJavaHelpers is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *   DistSysJavaHelpers is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 *  (C) Copyright 2012-2015, Gabor Kecskemeti (kecskemeti.gabor@sztaki.mta.hu)
 */

package hu.mta.sztaki.lpds.cloud.simulator.helpers.job;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Helper functionality for analyzing and comparing arbitrary list of jobs. With
 * the functions of this class one can determine the earliest/latest job in a
 * job-list.
 * 
 * @author 
 *         "Gabor Kecskemeti, Laboratory of Parallel and Distributed Systems, MTA SZTAKI (c) 2013-2015"
 */
public class JobListAnalyser {

	/**
	 * Determine the time instance upon which the first job in the list has got
	 * submitted.
	 * 
	 * @param joblist
	 *            the job-list to be analyzed.
	 * @return the earliest instance in time that has got mentioned in this
	 *         job-list.
	 */
	public static long getEarliestSubmissionTime(List<Job> joblist) {
		return Collections.min(joblist, JobListAnalyser.submitTimeComparator)
				.getSubmittimeSecs();
	}

	/**
	 * Determines the time instance that represents the last job's termination
	 * time in the entire list of jobs.
	 * 
	 * @param joblist
	 *            the job-list to be analyzed.
	 * @return the last time instance mentioned in this job-list
	 */
	public static long getLastTerminationTime(List<Job> joblist) {
		return Collections.max(joblist, JobListAnalyser.stopTimeComparator)
				.getStoptimeSecs();
	}
	
	/**
	 * Determine an estimation of the Application Performance Index of the job-list 
	 * in order to gauge user satisfaction. This should be queried only after all jobs have terminated.
	 * 
	 * @author Jamie Forster
	 * 
	 * @param joblist
	 * 				the job-list to be analyzed.
	 * 
	 * @param targetTime
	 * 				the targeted or expected process completion time.
	 * 
	 * @return the simulation's apdex rating between 0 and 1
	 * 
	 */
	public static double getApdex(List<Job> joblist, double targetTime) {
		double time; // Time taken
		int satisfied = 0, tolerant = 0, frustrated = 0; // Satisfaction zone counters
		
		// Get total satisfied/tolerating count
		for (int i = 0; i < joblist.size(); i++) {
			time = joblist.get(i).getRealqueueTime();
			
			if(time != -1) {
				// If a job has been queue'd, check if it has been simulated.
				if(joblist.get(i).getRealstopTime() != -1) {
					// If job has been simulated and completed, add its 'true' simulated execution time.
					time += joblist.get(i).getRealstopTime();
				}

				// Tally up the user satisfactions as according to Apdex satisfaction zone ranges
				if (time < targetTime) {
					satisfied++;
				} else if (time > targetTime && time < (targetTime * 4)) {
					tolerant++;
				} else {
					frustrated++;
				}
			}
			
		}

		// Calculate the ApDex rating and return it.
		return (double) (satisfied + (tolerant / 2)) / joblist.size();
	}

	/**
	 * A job comparator that allows the ordering of jobs based on their
	 * submission time instance.
	 */
	public static final Comparator<Job> submitTimeComparator = new Comparator<Job>() {
		@Override
		public int compare(Job o1, Job o2) {
			return Long.signum(o1.getSubmittimeSecs() - o2.getSubmittimeSecs());
		}
	};

	/**
	 * A job comparator that allows the ordering of jobs based on their
	 * termination time instance.
	 */
	public static final Comparator<Job> stopTimeComparator = new Comparator<Job>() {
		@Override
		public int compare(Job o1, Job o2) {
			return Long.signum(o1.getStoptimeSecs() - o2.getStoptimeSecs());
		}
	};


	/**
	 * A job comparator that allows the ordering of jobs based on their
	 * termination time instance.
	 */
	public static final Comparator<Job> startTimeComparator = new Comparator<Job>() {
		@Override
		public int compare(Job o1, Job o2) {
			return Long.signum(o1.getStartTimeInstance() - o2.getStartTimeInstance());
		}
	};

}
