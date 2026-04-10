import { useInfiniteQuery, useQuery } from '@tanstack/react-query'
import { getGraph, getGraphTimeline, getOpportunities, getSummary, getTriangles } from '@/lib/api'
import type {
  GraphParams,
  GraphTimelineParams,
  OpportunitiesParams,
  SummaryParams,
  TrianglesParams,
} from '@/types'

export function useSummary(params: SummaryParams) {
  return useQuery({
    queryKey: ['summary', params],
    queryFn: () => getSummary(params),
    staleTime: 5 * 60 * 1000,
  })
}

export function useGraph(params: GraphParams) {
  return useQuery({
    queryKey: ['graph', params],
    queryFn: () => getGraph(params),
    staleTime: 5 * 60 * 1000,
  })
}

export function useGraphTimeline(params: GraphTimelineParams) {
  return useQuery({
    queryKey: ['graph-timeline', params],
    queryFn: () => getGraphTimeline(params),
    staleTime: 5 * 60 * 1000,
  })
}

export function useTriangles(params: TrianglesParams) {
  return useQuery({
    queryKey: ['triangles', params],
    queryFn: () => getTriangles(params),
    enabled: !!params.currency,
    staleTime: 5 * 60 * 1000,
  })
}

export function useOpportunities(params: OpportunitiesParams) {
  return useInfiniteQuery({
    queryKey: ['opportunities', params],
    queryFn: ({ pageParam = 0 }) => getOpportunities({ ...params, offset: pageParam }),
    getNextPageParam: (lastPage) =>
      lastPage.meta.hasMore ? lastPage.meta.offset + lastPage.meta.limit : undefined,
    initialPageParam: 0,
    enabled: !!params.triangleKey,
  })
}
