import { type NextFunction, type Request, type Response, Router } from 'express'
import { ZodError } from 'zod'
import {
  getGraph,
  getGraphTimeline,
  getOpportunities,
  getSummary,
  getTriangles,
} from '../services/historical.js'
import {
  type ApiResponse,
  type GraphData,
  type GraphTimelineData,
  graphQuerySchema,
  graphTimelineQuerySchema,
  type OpportunityRow,
  opportunitiesQuerySchema,
  type PaginatedMeta,
  type SummaryData,
  summaryQuerySchema,
  type TriangleDetail,
  trianglesQuerySchema,
} from '../types/index.js'

const router = Router()

// Async handler wrapper
const asyncHandler =
  (fn: (req: Request, res: Response, next: NextFunction) => Promise<void>) =>
  (req: Request, res: Response, next: NextFunction) => {
    Promise.resolve(fn(req, res, next)).catch(next)
  }

// GET /summary
router.get(
  '/summary',
  asyncHandler(async (req, res) => {
    const startTime = Date.now()
    const query = summaryQuerySchema.parse(req.query)

    const data = await getSummary(query)

    const response: ApiResponse<SummaryData> = {
      data,
      meta: {
        startDate: query.startDate,
        endDate: query.endDate,
        queryTimeMs: Date.now() - startTime,
      },
    }
    res.json(response)
  })
)

// GET /graph
router.get(
  '/graph',
  asyncHandler(async (req, res) => {
    const startTime = Date.now()
    const query = graphQuerySchema.parse(req.query)

    const data = await getGraph(query)

    const response: ApiResponse<GraphData> & { meta: { nodeCount: number; linkCount: number } } = {
      data,
      meta: {
        startDate: query.startDate,
        endDate: query.endDate,
        queryTimeMs: Date.now() - startTime,
        nodeCount: data.nodes.length,
        linkCount: data.links.length,
      },
    }
    res.json(response)
  })
)

// GET /graph-timeline
router.get(
  '/graph-timeline',
  asyncHandler(async (req, res) => {
    const startTime = Date.now()
    const query = graphTimelineQuerySchema.parse(req.query)

    const data = await getGraphTimeline(query)

    const response: ApiResponse<GraphTimelineData> & {
      meta: { snapshotCount: number; allNodeCount: number; allLinkCount: number }
    } = {
      data,
      meta: {
        startDate: query.startDate,
        endDate: query.endDate,
        queryTimeMs: Date.now() - startTime,
        snapshotCount: data.snapshots.length,
        allNodeCount: data.allNodes.length,
        allLinkCount: data.allLinks.length,
      },
    }
    res.json(response)
  })
)

// GET /triangles
router.get(
  '/triangles',
  asyncHandler(async (req, res) => {
    const startTime = Date.now()
    const query = trianglesQuerySchema.parse(req.query)

    const { data, total } = await getTriangles(query)

    const response: { data: TriangleDetail[]; meta: PaginatedMeta } = {
      data,
      meta: {
        startDate: query.startDate,
        endDate: query.endDate,
        queryTimeMs: Date.now() - startTime,
        total,
        limit: query.limit,
        offset: query.offset,
        hasMore: query.offset + data.length < total,
      },
    }
    res.json(response)
  })
)

// GET /opportunities
router.get(
  '/opportunities',
  asyncHandler(async (req, res) => {
    const startTime = Date.now()
    const query = opportunitiesQuerySchema.parse(req.query)

    const { data, total } = await getOpportunities(query)

    const response: { data: OpportunityRow[]; meta: PaginatedMeta } = {
      data,
      meta: {
        startDate: query.startDate,
        endDate: query.endDate,
        queryTimeMs: Date.now() - startTime,
        total,
        limit: query.limit,
        offset: query.offset,
        hasMore: query.offset + data.length < total,
      },
    }
    res.json(response)
  })
)

// Error handler for this router
router.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
  if (err instanceof ZodError) {
    res.status(400).json({
      error: 'Validation error',
      details: err.issues,
    })
    return
  }

  console.error('Route error:', err)
  res.status(500).json({ error: 'Internal server error' })
})

export default router
