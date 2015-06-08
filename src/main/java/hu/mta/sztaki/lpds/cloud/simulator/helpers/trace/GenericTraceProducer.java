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

package hu.mta.sztaki.lpds.cloud.simulator.helpers.trace;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;

import java.util.List;

/**
 * Generic interface for job trace producers. Any implementation of this
 * interface should be able to provide a set of jobs at request (either with
 * fixed length or some arbitrary length derived from the particular
 * implementation's context).
 * 
 * @author 
 *         "Gabor Kecskemeti, Laboratory of Parallel and Distributed Systems, MTA SZTAKI (c) 2015"
 * 
 */
public interface GenericTraceProducer {
	/**
	 * This exception is supposed to be thrown on any occasion when there is no
	 * chance to receive any more jobs from a particular trace producer.
	 * 
	 * @author Gabor Kecskemeti (kecskemeti.gabor@sztaki.mta.hu)
	 * 
	 */
	public class NoFurtherJobsException extends Exception {
		private static final long serialVersionUID = 2345469377540263474L;

		/**
		 * This constructor allows to specify a textual description on the
		 * reason why there are no more jobs available from the trace producer.
		 * 
		 * @param cause
		 *            The textual description behind the unavailability of
		 *            further jobs.
		 */
		public NoFurtherJobsException(String cause) {
			super(cause);
		}
	}

	/**
	 * Allows to query a producer specific set of jobs. The size of the set
	 * should be determined during runtime by the implementation of the
	 * producer.
	 * 
	 * @return the list of jobs (the length of the job list will be arbitrary
	 *         from the caller point of view)
	 */
	public List<Job> getAllJobs();

	/**
	 * Allows the query of a specific number of jobs in a set.
	 * 
	 * @param num
	 *            The required size of the returning job set. (Please note: this
	 *            is a maximum length, in cases the implementor cannot provide
	 *            further jobs, then calling this function should result in an
	 *            exception.)
	 * @return the list of jobs (with a size of <= num)
	 * @throws NoFurtherJobsException
	 *             is thrown when the implementation of the interface run out of
	 *             jobs to produce in the array. Please keep in mind that this
	 *             exception is only supposed to be thrown if the job list's
	 *             length would be 0.
	 */
	public List<Job> getJobs(final int num) throws NoFurtherJobsException;
}
