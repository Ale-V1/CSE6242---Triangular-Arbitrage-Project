import { BigQuery } from '@google-cloud/bigquery'

const projectId = process.env.BIGQUERY_PROJECT_ID
const dataset = process.env.BIGQUERY_DATASET
const table = process.env.BIGQUERY_VIEW ?? 'vw_triangle_opportunities_enriched'
const location = process.env.BIGQUERY_LOCATION

const viewId = process.env.BIGQUERY_VIEW_ID ?? [projectId, dataset, table].filter(Boolean).join('.')

const credentials = {
  client_email: process.env.GOOGLE_APPLICATION_CREDENTIALS_EMAIL,
  private_key: process.env.GOOGLE_APPLICATION_CREDENTIALS_KEY, //?.replace(/\\n/g, '\n'),
}

if (!viewId) {
  throw new Error(
    'BigQuery configuration is required. Set BIGQUERY_VIEW_ID or BIGQUERY_PROJECT_ID and BIGQUERY_DATASET.'
  )
}

const viewIdPattern = /^[A-Za-z0-9_-]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$/
if (!viewIdPattern.test(viewId)) {
  throw new Error(
    'Invalid BigQuery view identifier. Expected BIGQUERY_VIEW_ID in project.dataset.view format.'
  )
}

const globalForBigQuery = globalThis as unknown as {
  bigquery?: BigQuery
}

export const bigquery =
  globalForBigQuery.bigquery ??
  new BigQuery({
    projectId,
    credentials,
  })

if (process.env.NODE_ENV !== 'production') {
  globalForBigQuery.bigquery = bigquery
}

export const historicalView = `\`${viewId}\``

export async function runQuery<T extends object>(
  query: string,
  params?: Record<string, unknown>
): Promise<T[]> {
  const [job] = await bigquery.createQueryJob({
    query,
    params,
    location,
    useLegacySql: false,
  })
  const [rows] = await job.getQueryResults()
  return rows as T[]
}
