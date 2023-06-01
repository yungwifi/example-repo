import {Callback, Context as AWSContext} from 'aws-lambda/handler'
import {sendEventBridgeCommand} from '../../utils/eventBridge'
import {MongoClient} from 'mongodb'
import {ImportFile, Status} from '../../interfaces'
import {Context, Logger} from '../app-core'
import {ImportFileRepo} from '../../repos'
import {sendImportEmail, sendImportFailedEmail} from '../sendMessage'
import {sendPusherOrganizationEvent} from '../../utils/eventPusher'

type Uipjob = {
  updated_at: string;
  source_id: string;
  status: string;
  total: number;
  tasks: {
    type: string;
    processed: number;
    matched_count: number;
  }[];
}
/**
 * AWSEvent Recursive handler to updated the import files status as it is processed by UIP.
 * @param awsEvent
 * @param awsContext
 * @param callback
 */
// eslint-disable-next-line max-len
export async function uipJobProcessor(awsEvent: {detail_type: string; detail: { jobId: string } }, awsContext: AWSContext, callback: Callback): Promise<any>{
  try{
    const importEvent: { id?: string; jobId?: string } = {
      ...awsEvent.detail,
    }
    const importMongoClient = new MongoClient(process.env.DATABASE_URL)
    const accountDbClient = new MongoClient(process.env.ACCOUNT_DB_URL)
    const context = new Context({mongoClient: importMongoClient, accountDbClient})
    const importRepo = new ImportFileRepo(context)
    const importFile = await importRepo.get(importEvent.id)

    if(importFile.status === Status.Cancelled){
      return
    }
    context.logger.log('Connecting to Database')
    await context.connectDbs()
    context.logger.log('Connecting to CDP Database and get job')
    const job = await getJobData(awsEvent.detail.jobId)

    const importUpdatedAt = new Date(importFile.updatedAt)
    const currentTime = new Date()

    context.logger.log('Processing Job')
    if(job){
      let jobUpdatedAt = new Date()
      if(job.updated_at){
        jobUpdatedAt = new Date(job.updated_at)
      }

      // Pusher event to send import progress
      if (job?.tasks?.length > 0) {
        await sendPusherOrganizationEvent(importFile.organizationId, 'ImportProgress', {
          id: importFile.id,
          progress: job.tasks.map(task => ({[task.type]: task['processed'] / job['total'] * 100})),
        })
        // Update Matched Records count based on job counter
        job.tasks.map(job  => {
          if (job.type === 'entity_resolution') {
            if (job.matched_count) {
              importFile.matchedRecords = job.matched_count
            }
          }
        })
        // Update Records count based on job counter
        importFile.records = job.total
      }
      // Check to make sure the Job is finished or Failed so it doesnt keep polling. Update status and keep polling
      if(checkJobStatus(job.status)){
        // update status and stop the recursive function
        await updateImportStatus(importRepo, importFile, job.status, job.source_id)
        if(job.status === 'Finished'){
          await sendImportUpdateEmail(context, importFile, Status.Complete)
        } else {
          await sendImportUpdateEmail(context, importFile, Status.Failed)
        }
        await context.closeDbs()
        return
      }

      // If the job is still within a 24 hour window and the status has changed, update the status
      if(checkTimeDifference(currentTime, importUpdatedAt, 24) && checkTimeDifference(currentTime, jobUpdatedAt,24)){
        let updatedImport = importFile
        if(importFile.status !== convertImportStatus(job.status)){
          updatedImport = await updateImportStatus(importRepo, importFile, job.status, job.source_id)
        }
        await repeatEvent(updatedImport)
        await context.closeDbs()
        return
      } else {
        await updateImportStatus(importRepo, importFile, 'Failed', job.source_id)
        await sendImportUpdateEmail(context, importFile, Status.Failed)
        await context.closeDbs()
        return
      }
    }
    // If UIP still hasnt process the job and it has been more than 15 minutes keep polling
    if(checkTimeDifference(currentTime, importUpdatedAt, .5)) {
      await repeatEvent(importEvent as ImportFile)
      await context.closeDbs()
      return
    } else {
      await updateImportStatus(importRepo, importFile, 'Failed', job.source_id)
      await sendImportUpdateEmail(context, importFile, Status.Failed)
      await context.closeDbs()
      return
    }
  } catch(e){
    const type: string = awsEvent.detail_type
    const importMongoClient = new MongoClient(process.env.DATABASE_URL)
    const accountDbClient = new MongoClient(process.env.ACCOUNT_DB_URL)
    const context = new Context({mongoClient: importMongoClient, accountDbClient})
    const message = `services.import.uipJobProcessor: ${JSON.stringify((e as Error).message)}. Event Detail: ${type}`
    context.logger.log(message)
    await context.closeDbs()
  }
}

/**
 * checkTimeDifference takes in the current time, the last updated timestamp from an import or a job and the difference
 * of hours you want to check. (This is important for checking for UIP receiving the job, it could take some time but we
 * dont want to let it run longer than 30 minutes) Then returns a boolean of whether or not it meats the criteria.
 * @param now
 * @param then
 * @param hours
 */
function checkTimeDifference(now: Date, then: Date, hours: number): boolean {
  const msBetweenDates = Math.abs(then.getTime() - now.getTime())
  const hoursBetweenDates = msBetweenDates / (60 * 60 * 1000)

  if (hoursBetweenDates < hours) {
    return true
  } else {
    return false
  }
}

/**
 * getJobData initializes the Data Platform mongo connections and gets the job in UIP associated with the Import based
 * on the s3 path of the import.
 * @param jobId
 */
async function getJobData(jobId: string): Promise<Uipjob> {
  try{
    const uipMongoClient = new MongoClient(process.env.UIP_DB_URL)
    const uipConnection = uipMongoClient.db('Metadata').collection('Jobs')
    await uipMongoClient.connect()
    const job = await uipConnection.findOne({job_id: jobId})
    await uipMongoClient.close()
    return job as unknown as Uipjob
  } catch(e){
    const message = `services.import.uipJobProcessor.getJobData: ${JSON.stringify((e as Error).message)}.`
    new Logger().log(message)
  }

}

/**
 * Repeat event
 * @param importEvent
 */
async function repeatEvent(importEvent: ImportFile): Promise<boolean> {
  return new Promise(resolve => setTimeout(async () => {
    await sendEventBridgeCommand('UipJobStatusRule', importEvent, 'import-export-service', 'UipJobStatusRule').then( eventBridge => {
      new Logger().log(`Event Bridge Message Sent, ${JSON.stringify(eventBridge)}`)
      resolve(true)
    })
  }, 3000))
}

/**
 * sendImportUpdateEmail
 * sends the success or failure of an import as an email to the user that initiated the import.
 * @param context
 * @param importEvent
 * @param status
 */
async function sendImportUpdateEmail(context: Context, importEvent: ImportFile, status: Status): Promise<void>{
  try{
    const user = await context.getUser(importEvent.createdById) as {email: string; firstName: string; lastName: string}
    const org = await context.getOrganization(importEvent.organizationId) as {name: string}

    if(status === Status.Failed){
      await sendImportFailedEmail(context, user.email, importEvent.name, user.firstName, user.lastName, org.name)
    } else {
      await sendImportEmail(context, user.email, importEvent.name, user.firstName, user.lastName, org.name)
    }
  } catch(e){
    const message = `services.import.uipJobProcessor.sendImportUpdateEmail: ${JSON.stringify((e as Error).message)}.`
    context.logger.log(message)
  }

}

/**
 * updateImportStatus takes in the mongo connection, the import object and the status and updated the status and updatedAt
 * time stamp.
 * @param importRepo
 * @param importFile
 * @param status
 */
// eslint-disable-next-line max-len
async function updateImportStatus(importRepo: ImportFileRepo, importFile: ImportFile, status: string, sourceId: string): Promise<ImportFile> {
  try{
    importFile.status = convertImportStatus(status)
    importFile.updatedAt = new Date()
    importFile.sourceId = sourceId
    const updateImport = await importRepo.update(importFile)
    await sendPusherOrganizationEvent(importFile.organizationId, 'ImportComplete', {
      id: importFile.id,
      status: importFile.status,
      records: importFile.records,
      matchedRecords: importFile.matchedRecords,
      description: importFile.description,
    })
    return updateImport
  } catch (e) {
    const message = `services.import.uipJobProcessor.updateImportStatus: ${JSON.stringify((e as Error).message)}.`
    new Logger().log(message)
  }

}

/**
 * checkJobStatus takes in a job status and checks to see if matches any of the statuses that should stop the recursive
 * function.
 * @param status
 */
function checkJobStatus(status: string): boolean {
  // TODO need to update these statuses from the core-data library when they are added there.
  const statuses = [
    'Failed Validation',
    'Finished',
    'Deleted',
    'Failed',
  ]
  return statuses.includes(status)
}

/**
 * Convert status is responsible for taking the status from UIP and mapping it to the status in the import export service enum
 * @param status
 */
function convertImportStatus(status: string): Status {
  switch(status) {
    case 'Failed':
    case 'Failed Validation':
      return Status.Failed
    case 'Deleted':
      return Status.Deleted
    case 'Finished':
      return Status.Complete
    case 'Stopped':
    case 'Running':
    case 'Validating':
      return Status.Processing
    case 'Not Started':
      return Status.Draft
    default:
      return Status.Upload
  }
}
