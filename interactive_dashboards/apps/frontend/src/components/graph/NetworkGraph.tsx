import * as d3 from 'd3'
import { useCallback, useEffect, useRef, useState } from 'react'
import type {
  D3Link,
  D3Node,
  GraphLink,
  GraphNode,
  GraphSnapshot,
  OpportunityCategory,
} from '@/types'
import { GraphTooltip } from './GraphTooltip'

interface NetworkGraphProps {
  // For animation - current time bucket's data (historical mode)
  currentSnapshot?: GraphSnapshot | null
  // Props for live mode
  nodes?: GraphNode[]
  links?: GraphLink[]
  selectedNode: string | null
  onNodeClick: (nodeId: string) => void
  onNodeHover?: (nodeId: string | null) => void
  mode: 'historical' | 'live'
  highlightedEdges?: Map<string, { status: 'active' | 'stale'; category: OpportunityCategory }>
}

interface TooltipData {
  type: 'node' | 'link'
  data: GraphNode | GraphLink
  x: number
  y: number
}

const TRANSITION_DURATION = 300

function edgeColor(
  state: { status: 'active' | 'stale'; category: OpportunityCategory } | undefined
): string {
  if (!state) return '#6b7280'
  // Brighter green for network, amber matches feed
  return state.category === 'near-miss' ? '#f5c854' : '#1aff66'
}

function edgeOpacity(
  state: { status: 'active' | 'stale'; category: OpportunityCategory } | undefined
): number {
  if (!state) return 0.5
  if (state.category === 'near-miss') return state.status === 'active' ? 0.25 : 0.15
  return state.status === 'active' ? 1 : 0.4
}

function edgeClass(
  state: { status: 'active' | 'stale'; category: OpportunityCategory } | undefined
): string {
  if (!state) return ''
  if (state.status === 'active') return 'edge-flash'
  return 'edge-stale'
}

/** Sort order for SVG paint order (higher = rendered on top). */
function edgeSortOrder(
  state: { status: 'active' | 'stale'; category: OpportunityCategory } | undefined
): number {
  if (!state) return 0
  if (state.category === 'near-miss') return state.status === 'active' ? 2 : 1
  return state.status === 'active' ? 4 : 3 // profitable on top
}

export function NetworkGraph({
  currentSnapshot,
  nodes: legacyNodes,
  links: legacyLinks,
  selectedNode,
  onNodeClick,
  onNodeHover,
  mode,
  highlightedEdges,
}: NetworkGraphProps) {
  const svgRef = useRef<SVGSVGElement>(null)
  const simulationRef = useRef<d3.Simulation<D3Node, D3Link> | null>(null)
  const containerRef = useRef<d3.Selection<SVGGElement, unknown, null, undefined> | null>(null)
  const nodesRef = useRef<Map<string, D3Node>>(new Map())
  const [tooltip, setTooltip] = useState<TooltipData | null>(null)

  // For live mode, use legacy props
  const liveNodes = legacyNodes ?? []
  const liveLinks = legacyLinks ?? []

  const handleNodeClick = useCallback(
    (nodeId: string) => {
      onNodeClick(nodeId)
    },
    [onNodeClick]
  )

  // Initialize SVG container and zoom - runs once
  useEffect(() => {
    if (!svgRef.current) return

    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()

    const container = svg.append('g')
    containerRef.current = container

    // Create link and node groups
    container.append('g').attr('class', 'links')
    container.append('g').attr('class', 'nodes')

    const zoom = d3
      .zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.1, 4])
      .on('zoom', (event) => {
        container.attr('transform', event.transform)
      })

    svg.call(zoom)

    return () => {
      if (simulationRef.current) {
        simulationRef.current.stop()
      }
    }
  }, [])

  // Live mode simulation - similar to original behavior
  useEffect(() => {
    if (mode !== 'live' || !svgRef.current || !containerRef.current || liveNodes.length === 0)
      return

    const container = containerRef.current
    const width = svgRef.current.clientWidth
    const height = svgRef.current.clientHeight

    // Prepare data
    const d3Nodes: D3Node[] = liveNodes.map((n) => {
      const existing = nodesRef.current.get(n.id)
      return {
        ...n,
        x: existing?.x ?? width / 2 + (Math.random() - 0.5) * 100,
        y: existing?.y ?? height / 2 + (Math.random() - 0.5) * 100,
        fx: existing?.fx,
        fy: existing?.fy,
      }
    })
    const nodeIdSet = new Set(d3Nodes.map((n) => n.id))
    const d3Links: D3Link[] = liveLinks
      .filter((l) => nodeIdSet.has(l.source) && nodeIdSet.has(l.target))
      .map((l) => ({ ...l }))

    // Update nodesRef
    for (const node of d3Nodes) {
      nodesRef.current.set(node.id, node)
    }

    // Scales
    const maxVolume = Math.max(...liveNodes.map((n) => n.totalVolumeUsd), 1)
    const radiusScale = d3.scaleSqrt().domain([0, maxVolume]).range([12, 20])
    const widthScale = d3
      .scaleLog()
      .domain([1, Math.max(...d3Links.map((l) => l.frequency + 1), 2)])
      .range([1.5, 7])

    // Determine node colors based on connected edge categories
    const nodeColorMap = new Map<string, string>()
    if (highlightedEdges && highlightedEdges.size > 0) {
      // First pass: identify which nodes have profitable edges
      const hasProfit = new Set<string>()
      const hasNearMiss = new Set<string>()
      for (const [pair, state] of highlightedEdges) {
        const link = d3Links.find((l) => l.pair === pair)
        if (link) {
          if (state.category === 'profitable') {
            hasProfit.add(typeof link.source === 'string' ? link.source : link.source.id)
            hasProfit.add(typeof link.target === 'string' ? link.target : link.target.id)
          } else {
            hasNearMiss.add(typeof link.source === 'string' ? link.source : link.source.id)
            hasNearMiss.add(typeof link.target === 'string' ? link.target : link.target.id)
          }
        }
      }
      // Color nodes: green if profitable, amber if only near-miss, gray if neither
      for (const node of d3Nodes) {
        if (hasProfit.has(node.id)) {
          nodeColorMap.set(node.id, '#1aff66')
        } else if (hasNearMiss.has(node.id)) {
          nodeColorMap.set(node.id, '#f5c854')
        } else {
          nodeColorMap.set(node.id, '#6b7280')
        }
      }
    } else {
      // No highlighted edges: all nodes gray
      for (const node of d3Nodes) {
        nodeColorMap.set(node.id, '#6b7280')
      }
    }

    // Stop existing simulation
    if (simulationRef.current) {
      simulationRef.current.stop()
    }

    // Create simulation
    const simulation = d3
      .forceSimulation<D3Node>(d3Nodes)
      .force(
        'link',
        d3
          .forceLink<D3Node, D3Link>(d3Links)
          .id((d) => d.id)
          .distance(100)
      )
      .force('charge', d3.forceManyBody().strength(-300))
      .force('center', d3.forceCenter(width / 2, height / 2))
      .force(
        'collision',
        d3.forceCollide<D3Node>().radius((d) => radiusScale(d.totalVolumeUsd) + 8)
      )
      .alphaMin(0.001)
      .alphaDecay(0.0005)

    simulationRef.current = simulation

    // Create drag behavior
    const dragBehavior = d3
      .drag<SVGGElement, D3Node>()
      .on('start', (event, d) => {
        if (!event.active) simulation.alphaTarget(0.3).restart()
        d.fx = d.x
        d.fy = d.y
      })
      .on('drag', (event, d) => {
        d.fx = event.x
        d.fy = event.y
        nodesRef.current.set(d.id, d)
      })
      .on('end', (event, d) => {
        if (!event.active) simulation.alphaTarget(0)
        d.fx = null
        d.fy = null
        nodesRef.current.set(d.id, d)
      })

    // Update links - use D3-bound data directly for tooltip
    container
      .select('.links')
      .selectAll<SVGLineElement, D3Link>('line')
      .data(d3Links, (d) => d.pair)
      .join('line')
      .attr('data-pair', (d) => d.pair)
      .attr('stroke', (d) => edgeColor(highlightedEdges?.get(d.pair)))
      .attr('stroke-width', (d) => widthScale(d.frequency + 1))
      .attr('stroke-opacity', (d) => edgeOpacity(highlightedEdges?.get(d.pair)))
      .attr('stroke-linecap', 'round')
      .attr('class', (d) => edgeClass(highlightedEdges?.get(d.pair)))
      .on('mouseenter', (event, d) => {
        // Extract GraphLink-compatible data (source/target may be D3Node objects after simulation)
        const linkData: GraphLink = {
          source: typeof d.source === 'string' ? d.source : d.source.id,
          target: typeof d.target === 'string' ? d.target : d.target.id,
          pair: d.pair,
          frequency: d.frequency,
          avgProfit: d.avgProfit,
          totalVolumeUsd: d.totalVolumeUsd,
        }
        setTooltip({ type: 'link', data: linkData, x: event.pageX, y: event.pageY })
      })
      .on('mouseleave', () => setTooltip(null))

    // Reorder edges so profitable ones render on top of near-misses
    if (highlightedEdges) {
      container
        .select('.links')
        .selectAll<SVGLineElement, D3Link>('line')
        .sort((a, b) => {
          const aOrder = edgeSortOrder(highlightedEdges.get(a.pair))
          const bOrder = edgeSortOrder(highlightedEdges.get(b.pair))
          return aOrder - bOrder
        })
    }

    // Update nodes - separate enter for structure, then apply handlers to all
    const nodeGroups = container
      .select('.nodes')
      .selectAll<SVGGElement, D3Node>('g')
      .data(d3Nodes, (d) => d.id)
      .join(
        (enter) => {
          const g = enter
            .append('g')
            .attr('data-id', (d) => d.id)
            .attr('cursor', 'pointer')

          g.append('circle')
            .attr('r', (d) => radiusScale(d.totalVolumeUsd))
            .attr('fill', (d) => nodeColorMap.get(d.id) || '#6b7280')
            .attr('fill-opacity', 0.95)
            .attr('stroke', 'var(--foreground)')
            .attr('stroke-opacity', (d) => (d.id === selectedNode ? 0.95 : 0.65))
            .attr('stroke-width', (d) => (d.id === selectedNode ? 3 : 2.25))

          g.append('text')
            .text((d) => d.id)
            .attr('text-anchor', 'middle')
            .attr('dy', '.35em')
            .attr('fill', 'var(--foreground)')
            .attr('stroke', 'var(--background)')
            .attr('stroke-width', 3)
            .attr('paint-order', 'stroke')
            .attr('font-size', (d) => Math.min(radiusScale(d.totalVolumeUsd) * 0.5, 14))
            .attr('font-weight', 'bold')
            .attr('pointer-events', 'none')

          return g
        },
        (update) => update,
        (exit) => exit.remove()
      )

    // Update circle radius, colors, and text size for ALL nodes (enter + update) when data changes
    nodeGroups
      .select('circle')
      .attr('r', (d) => radiusScale(d.totalVolumeUsd))
      .attr('fill', (d) => nodeColorMap.get(d.id) || '#6b7280')

    nodeGroups
      .select('text')
      .attr('font-size', (d) => Math.min(radiusScale(d.totalVolumeUsd) * 0.5, 14))

    // Apply event handlers to ALL nodes (both new and existing) so they use current data
    nodeGroups
      .call(dragBehavior)
      .on('click', (_, d) => handleNodeClick(d.id))
      .on('mouseenter', (event, d) => {
        // Use d directly - D3 updates bound data, so d has current values
        setTooltip({ type: 'node', data: d, x: event.pageX, y: event.pageY })
        onNodeHover?.(d.id)
      })
      .on('mouseleave', () => {
        setTooltip(null)
        onNodeHover?.(null)
      })

    // Update positions on tick
    simulation.on('tick', () => {
      container
        .select('.links')
        .selectAll<SVGLineElement, D3Link>('line')
        .attr('x1', (d) => (d.source as D3Node).x!)
        .attr('y1', (d) => (d.source as D3Node).y!)
        .attr('x2', (d) => (d.target as D3Node).x!)
        .attr('y2', (d) => (d.target as D3Node).y!)

      nodeGroups.attr('transform', (d) => {
        nodesRef.current.set(d.id, d)
        return `translate(${d.x},${d.y})`
      })
    })

    return () => {
      simulation.stop()
    }
  }, [liveNodes, liveLinks, mode, handleNodeClick, onNodeHover, highlightedEdges, selectedNode])

  // Historical mode - dynamic simulation based on current snapshot
  useEffect(() => {
    if (mode !== 'historical' || !svgRef.current || !containerRef.current || !currentSnapshot)
      return

    const container = containerRef.current
    const width = svgRef.current.clientWidth
    const height = svgRef.current.clientHeight

    // Use current snapshot's nodes and links
    const snapshotNodes = currentSnapshot.nodes
    const snapshotLinks = currentSnapshot.links

    if (snapshotNodes.length === 0) {
      // Clear the graph if no nodes
      container.select('.nodes').selectAll('*').remove()
      container.select('.links').selectAll('*').remove()
      return
    }

    // Prepare D3 nodes - preserve existing positions
    const d3Nodes: D3Node[] = snapshotNodes.map((n) => {
      const existing = nodesRef.current.get(n.id)
      return {
        ...n,
        x: existing?.x ?? width / 2 + (Math.random() - 0.5) * 200,
        y: existing?.y ?? height / 2 + (Math.random() - 0.5) * 200,
        fx: existing?.fx,
        fy: existing?.fy,
      }
    })

    const nodeIdSet = new Set(d3Nodes.map((n) => n.id))
    const d3Links: D3Link[] = snapshotLinks
      .filter((l) => nodeIdSet.has(l.source) && nodeIdSet.has(l.target))
      .map((l) => ({ ...l }))

    // Scales based on current snapshot
    const maxVolume = Math.max(...snapshotNodes.map((n) => n.totalVolumeUsd), 1)
    const radiusScale = d3.scaleSqrt().domain([0, maxVolume]).range([8, 40])
    const maxFrequency = Math.max(...d3Links.map((l) => l.frequency), 1)
    const colorScale = d3.scaleSequential(d3.interpolateViridis).domain([0, maxFrequency])
    const widthScale = d3
      .scaleLog()
      .domain([1, Math.max(...d3Links.map((l) => l.frequency + 1), 2)])
      .range([1.5, 7])

    // Stop existing simulation
    if (simulationRef.current) {
      simulationRef.current.stop()
    }

    // Create new simulation with snapshot nodes
    const simulation = d3
      .forceSimulation<D3Node>(d3Nodes)
      .force(
        'link',
        d3
          .forceLink<D3Node, D3Link>(d3Links)
          .id((d) => d.id)
          .distance(100)
      )
      .force('charge', d3.forceManyBody().strength(-300))
      .force('center', d3.forceCenter(width / 2, height / 2))
      .force(
        'collision',
        d3.forceCollide<D3Node>().radius((d) => radiusScale(d.totalVolumeUsd) + 8)
      )
      .alphaDecay(0.02) // Faster settling for smoother animation

    simulationRef.current = simulation

    // Create drag behavior
    const dragBehavior = d3
      .drag<SVGGElement, D3Node>()
      .on('start', (event, d) => {
        if (!event.active) simulation.alphaTarget(0.3).restart()
        d.fx = d.x
        d.fy = d.y
      })
      .on('drag', (event, d) => {
        d.fx = event.x
        d.fy = event.y
        nodesRef.current.set(d.id, d)
      })
      .on('end', (event, d) => {
        if (!event.active) simulation.alphaTarget(0)
        d.fx = null
        d.fy = null
        nodesRef.current.set(d.id, d)
      })

    // Update links with enter/update/exit
    const linkSelection = container
      .select('.links')
      .selectAll<SVGLineElement, D3Link>('line')
      .data(d3Links, (d) => d.pair)

    // Remove exiting links
    linkSelection
      .exit()
      .transition()
      .duration(TRANSITION_DURATION)
      .attr('stroke-opacity', 0)
      .remove()

    // Add entering links
    const linkEnter = linkSelection
      .enter()
      .append('line')
      .attr('data-pair', (d) => d.pair)
      .attr('stroke', (d) => colorScale(d.frequency))
      .attr('stroke-width', (d) => widthScale(d.frequency + 1))
      .attr('stroke-opacity', 0)
      .attr('stroke-linecap', 'round')
      .on('mouseenter', (event, d) => {
        const originalLink = snapshotLinks.find((l) => l.pair === d.pair)
        if (originalLink) {
          setTooltip({ type: 'link', data: originalLink, x: event.pageX, y: event.pageY })
        }
      })
      .on('mouseleave', () => setTooltip(null))

    // Merge and transition
    linkEnter
      .merge(linkSelection)
      .transition()
      .duration(TRANSITION_DURATION)
      .attr('stroke', (d) => colorScale(d.frequency))
      .attr('stroke-width', (d) => widthScale(d.frequency + 1))
      .attr('stroke-opacity', 0.8)

    // Update nodes with enter/update/exit
    const nodeSelection = container
      .select('.nodes')
      .selectAll<SVGGElement, D3Node>('g')
      .data(d3Nodes, (d) => d.id)

    // Remove exiting nodes with fade out
    nodeSelection.exit().transition().duration(TRANSITION_DURATION).attr('opacity', 0).remove()

    // Add entering nodes
    const nodeEnter = nodeSelection
      .enter()
      .append('g')
      .attr('data-id', (d) => d.id)
      .attr('cursor', 'pointer')
      .attr('opacity', 0)
      .attr('transform', (d) => `translate(${d.x},${d.y})`)
      .call(dragBehavior)
      .on('click', (_, d) => handleNodeClick(d.id))
      .on('mouseenter', (event, d) => {
        const originalNode = snapshotNodes.find((n) => n.id === d.id)
        if (originalNode) {
          setTooltip({ type: 'node', data: originalNode, x: event.pageX, y: event.pageY })
        }
        onNodeHover?.(d.id)
      })
      .on('mouseleave', () => {
        setTooltip(null)
        onNodeHover?.(null)
      })

    nodeEnter
      .append('circle')
      .attr('r', (d) => radiusScale(d.totalVolumeUsd))
      .attr('fill', (d) => (d.id === selectedNode ? 'var(--chart-2)' : 'var(--primary)'))
      .attr('fill-opacity', 0.95)
      .attr('stroke', 'var(--foreground)')
      .attr('stroke-opacity', (d) => (d.id === selectedNode ? 0.95 : 0.65))
      .attr('stroke-width', (d) => (d.id === selectedNode ? 3 : 2.25))

    nodeEnter
      .append('text')
      .text((d) => d.id)
      .attr('text-anchor', 'middle')
      .attr('dy', '.35em')
      .attr('fill', 'var(--foreground)')
      .attr('stroke', 'var(--background)')
      .attr('stroke-width', 3)
      .attr('paint-order', 'stroke')
      .attr('font-size', (d) => Math.min(radiusScale(d.totalVolumeUsd) * 0.6, 12))
      .attr('font-weight', 'bold')
      .attr('pointer-events', 'none')

    // Merge entering and updating nodes
    const allNodes = nodeEnter.merge(nodeSelection)

    // Transition opacity for entering nodes
    allNodes.transition().duration(TRANSITION_DURATION).attr('opacity', 1)

    // Update circle sizes for existing nodes
    allNodes
      .select('circle')
      .transition()
      .duration(TRANSITION_DURATION)
      .attr('r', (d) => radiusScale(d.totalVolumeUsd))
      .attr('fill', (d) => (d.id === selectedNode ? 'var(--chart-2)' : 'var(--primary)'))

    // Update text sizes
    allNodes
      .select('text')
      .transition()
      .duration(TRANSITION_DURATION)
      .attr('font-size', (d) => Math.min(radiusScale(d.totalVolumeUsd) * 0.6, 12))

    // Get merged selections for tick updates
    const allLinksSelection = container.select('.links').selectAll<SVGLineElement, D3Link>('line')

    // Update positions on tick
    simulation.on('tick', () => {
      allLinksSelection
        .attr('x1', (d) => (d.source as D3Node).x!)
        .attr('y1', (d) => (d.source as D3Node).y!)
        .attr('x2', (d) => (d.target as D3Node).x!)
        .attr('y2', (d) => (d.target as D3Node).y!)

      allNodes.attr('transform', (d) => {
        // Save position to nodesRef for persistence
        nodesRef.current.set(d.id, d)
        return `translate(${d.x},${d.y})`
      })
    })

    // Cleanup function
    return () => {
      // Don't stop simulation here - let it continue for smooth transitions
    }
  }, [currentSnapshot, mode, handleNodeClick, onNodeHover, selectedNode])

  // Update selected node styling without rebuilding the simulation
  useEffect(() => {
    if (!svgRef.current || !containerRef.current) return

    containerRef.current
      .select('.nodes')
      .selectAll<SVGGElement, D3Node>('g')
      .select('circle')
      .attr('fill', (d) => (d.id === selectedNode ? 'var(--chart-2)' : 'var(--primary)'))
      .attr('stroke-opacity', (d) => (d.id === selectedNode ? 0.95 : 0.65))
      .attr('stroke-width', (d) => (d.id === selectedNode ? 3 : 2.25))
  }, [selectedNode])

  // Update highlighted edges without re-creating the simulation
  useEffect(() => {
    if (!svgRef.current || !containerRef.current || !highlightedEdges) return

    const linksGroup = containerRef.current.select('.links')

    linksGroup
      .selectAll<SVGLineElement, D3Link>('line')
      .attr('stroke', (d) => edgeColor(highlightedEdges.get(d.pair)))
      .attr('stroke-opacity', (d) => edgeOpacity(highlightedEdges.get(d.pair)))
      .attr('class', (d) => edgeClass(highlightedEdges.get(d.pair)))
      .sort((a, b) => {
        const aOrder = edgeSortOrder(highlightedEdges.get(a.pair))
        const bOrder = edgeSortOrder(highlightedEdges.get(b.pair))
        return aOrder - bOrder
      })
  }, [highlightedEdges])

  return (
    <div className="relative h-full w-full">
      <svg ref={svgRef} className="h-full w-full" />
      {tooltip && (
        <GraphTooltip type={tooltip.type} data={tooltip.data} x={tooltip.x} y={tooltip.y} />
      )}
    </div>
  )
}
