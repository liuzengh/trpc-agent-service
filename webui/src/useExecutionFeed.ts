import { useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getClaims, getExecution } from './api'
import {
  claimFingerprint,
  executionKey,
  mergeClaimsWithExecutionDetails,
  nextSelectedKey,
  runSettled,
} from './execution'
import type { Claim } from './types'

const POLL_INTERVAL_MS = 5000

export function useExecutionFeed(tenant: string, appCode = '') {
  const [selectedKey, setSelectedKey] = useState('')

  const claimsQuery = useQuery({
    queryKey: ['console', 'claims', tenant, appCode],
    queryFn: ({ signal }) => getClaims(tenant, appCode, signal),
    enabled: Boolean(tenant),
    refetchInterval: POLL_INTERVAL_MS,
    staleTime: 0,
  })

  const rawClaims: Claim[] = claimsQuery.data ?? []

  useEffect(() => {
    setSelectedKey((current) => nextSelectedKey(rawClaims, current))
  }, [rawClaims])

  const selectedRawClaim = rawClaims.find((claim) => executionKey(claim) === selectedKey)
  const selectedFingerprint = claimFingerprint(selectedRawClaim)
  const selectedChannel = selectedRawClaim?.channel ?? ''
  const selectedBindingID = selectedRawClaim?.binding_id ?? ''
  const selectedMessageID = selectedRawClaim?.message_id ?? ''

  const detailQuery = useQuery({
    queryKey: [
      'console',
      'execution',
      tenant,
      selectedChannel,
      selectedBindingID,
      selectedMessageID,
      selectedFingerprint,
    ],
    queryFn: ({ signal }) => getExecution(
      tenant,
      selectedChannel,
      selectedBindingID,
      selectedMessageID,
      '',
      signal,
    ),
    enabled: Boolean(tenant && selectedRawClaim),
    refetchInterval: (query) => {
      const detail = query.state.data
      return detail && runSettled(detail) ? false : POLL_INTERVAL_MS
    },
    staleTime: 0,
  })

  const claims = useMemo(() => {
    if (!selectedRawClaim || !detailQuery.data) return rawClaims
    return mergeClaimsWithExecutionDetails(rawClaims, {
      [executionKey(selectedRawClaim)]: detailQuery.data,
    })
  }, [detailQuery.data, rawClaims, selectedRawClaim])

  const selectedClaim = claims.find((claim) => executionKey(claim) === selectedKey)
  const error = claimsQuery.error ?? detailQuery.error

  return {
    claims,
    selectedKey,
    selectedClaim,
    detail: detailQuery.data ?? null,
    setSelectedKey,
    error: error instanceof Error ? error.message : '',
    loadingList: claimsQuery.isLoading,
    loadingDetail: detailQuery.isLoading,
    refresh: () => {
      void claimsQuery.refetch()
      if (selectedRawClaim) void detailQuery.refetch()
    },
  }
}
