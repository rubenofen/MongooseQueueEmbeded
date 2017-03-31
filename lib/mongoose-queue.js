'use strict';

/**
 * Dependencies
 */
var os = require('os');
var mongoose = require('mongoose');
var Schema = mongoose.Schema;
var _ = require('underscore');
var JobSchema = require('../schemas/job-schema');

/**
 * Implements a queue based on mongoose. 
 * 
 * @class MongooseQueue
 */
class MongooseQueue {
	/**
	 * Creates an instance of MongooseQueue.
	 * 
	 * @param {String} payloadModel
	 * @param {string} [workerId='']
	 * @param {Object} [options={}]
	 */
	constructor(payloadModel, workerId = '', options = {}) {
		this.payloadModel = payloadModel;

		this.workerHostname = os.hostname();
		this.workerId = workerId;

		this.options = _.defaults(options, {
			payloadRefType: Schema.Types.ObjectId,
			queueCollection: 'queue',
			blockDuration: 30000,
			maxRetries: 5,
			maxJobsInProcess: 1
		});

		// create job model
		this.JobModel = JobSchema(this.options.queueCollection, this.options.payloadRefType);
	}

	/**
	 * Adds an element to the queue.
	 * 
	 * @param {any} payload				- The payload to attach to this job. This needs to be a Mongoose document.
	 * @param {fn(err, jobId)} cb		- Callback either returning an error or the id of the job added to the queue.
	 */
	add(payload, cb) {
		// check if payload is a mongoose document
		if (!payload)
			return cb(new Error('Payload missing.'), null);

		// add to queue
		var newJob = new this.JobModel({
				payload: payload
			})
			.save(function (err, job) {
				/* istanbul ignore if */
				if (err)
					cb(err, null);
				else
					cb(null, job._id.toString());
			});
	}

	/**
	 * Get a job from the queue that is not done and not currentlich blocked. 
	 * 
	 * @param {fn(err, job)} cb	- Callback with error or job fetched from queue for processing.
	 */
	get(cb) {
		// fetch the oldest job from the queue that
		// is not blocked, is not done
		// then increase blockedUntil and return it
		let options = this.options;
		let maxJobs = options.maxJobsInProcess;
		let maxRetries = options.maxRetries;
		let blockDuration = options.blockDuration;

		let model = this.JobModel;
		let workerId = this.workerId;
		let workerHostname = this.workerHostname;
		model.
		count({
			process: true
		}, function (err, tasksInProcess) {

			if (tasksInProcess < maxJobs) {
				model
					.findOneAndUpdate({
						blockedUntil: {
							$lt: Date.now()
						},
						retries: {
							$lte: maxRetries
						},
						done: false,
						process: false
					}, {
						$set: {
							blockedUntil: new Date(Date.now() + blockDuration),
							workerId: workerId,
							workerHostname: workerHostname,
							process: true
						},
						$inc: {
							retries: 1
						},
					}, {
						new: true,
						sort: {
							createdAt: 1
						}
					})
					.exec(function (err, job) {
						/* istanbul ignore if */
						if (err)
							return cb(err, null);
						else if (!job)
							return cb(null, null);
						else {
							cb(null, {
								id: job._id,
								payload: job.payload,
								blockedUntil: job.blockedUntil,
								done: job.done
							});
						}
					});
			} else {
				return cb(new Error("Too jobs in process status, only " + maxJobs + " is allowed. Please change the options if you need more."), null);
			}
		});

	}

	/**
	 * Mark a job as done. 
	 * 
	 * @param {String} jobId 		- Id of the job to mark as done.
	 * @param {fn(err, job)} cb		- Callback with error or updated job.
	 */
	ack(jobId, cb) {
		this.JobModel.findOneAndUpdate({
			_id: jobId
		}, {
			$set: {
				done: true
			}
		}, {
			new: true
		}, function (err, job) {
			/* istanbul ignore if */
			if (err)
				return cb(err, null);
			else if (!job)
				return cb(new Error('Job id invalid, job not found.'), null);
			else
				cb(null, {
					id: job._id,
					payload: job.payload,
					blockedUntil: job.blockedUntil,
					done: job.done
				});
		});
	}

	/**
	 * Mark a job done with an error message. 
	 * 
	 * @param {String} jobId	- Id of the job to mark with error.
	 * @param {String} error	- Error message
	 * @param {fn(err, job)} cb	- Callback with error or updated job.
	 */
	error(jobId, error, cb) {
		this.JobModel.findOneAndUpdate({
			_id: jobId
		}, {
			$set: {
				done: true,
				error: error
			}
		}, {
			new: true
		}, function (err, job) {
			/* istanbul ignore if */
			if (err)
				return cb(err, null);
			else if (!job)
				return cb(new Error('Job id invalid, job not found.'), null);
			else
				cb(null, {
					id: job._id,
					payload: job.payload,
					blockedUntil: job.blockedUntil,
					done: job.done,
					error: job.error
				});
		});
	}

	/**
	 * Removes all jobs from the queue that are marked done (done/error) or reached the maximum retry count. 
	 * 
	 * @param {fn(err)} cb - Callback with null when successful, otherwise the error is passed.
	 */
	clean(cb) {
		this.JobModel.remove({
			$or: [{
					done: true
				},
				{
					retries: {
						$gt: this.options.maxRetries
					}
				}
			]
		}, function (err) {
			/* istanbul ignore if */
			if (err)
				return cb(err);
			else
				cb(null);
		});
	}

	/**
	 * Removes ALL jobs from the queue. 
	 * 
	 * @param {fn(err)} cb - Callback with null when successful, otherwise the error is passed.
	 */
	reset(cb) {
		this.JobModel.remove({}, function (err) {
			/* istanbul ignore if */
			if (err)
				return cb(err);
			else
				cb(null);
		});
	}

	recoverProcessTasks(cb) {
		this.JobModel
			.update({
				done: false,
				process: true
			}, {
				$set: {
					blockedUntil: new Date(Date.now() + this.options.blockDuration),
					workerId: this.workerId,
					workerHostname: this.workerHostname,
					process: false
				},
				$inc: {
					retries: 1
				},
			}, {
				new: true,
				sort: {
					createdAt: 1
				},
				multi: true
			})
			.exec(function (err, result) {
				/* istanbul ignore if */
				if (err)
					return cb(err, null);
				else if (!result)
					return cb(null, null);
				else {
					cb(null, result);
				}
			});
	}
}

module.exports = MongooseQueue;