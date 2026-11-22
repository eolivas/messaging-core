// DTOs mirror the BFF (Demo.Application) wire format: camelCase JSON, enums as strings.

export interface ProviderDto {
  id: string;
  displayName: string;
  supported: boolean;
  unsupportedReason: string | null;
}

export interface SettingEntryDto {
  group: string;
  name: string;
  value: string;
  isSecret: boolean;
  editable: boolean;
  permittedValuesOrRange: string | null;
}

export interface ProviderDefaultsDto {
  providerId: string;
  messagingMode: string;
  settings: SettingEntryDto[];
}

export interface SettingErrorDto {
  field: string;
  message: string;
  permittedValuesOrRange: string | null;
}

export interface ValidateSettingsRequest {
  overrides: Record<string, string>;
}

export interface ValidateSettingsResponse {
  valid: boolean;
  errors: SettingErrorDto[];
}

export type SessionState = 'Idle' | 'Starting' | 'Active' | 'Stopping';

export interface ServiceStatusDto {
  service: string;
  role: string;
  ready: boolean;
}

export interface ProblemSummaryDto {
  title: string;
  detail: string;
}

export interface SessionStatusDto {
  state: SessionState;
  providerId: string | null;
  providerName: string | null;
  health: string;
  services: ServiceStatusDto[];
  lastError: ProblemSummaryDto | null;
}

export interface StartSessionRequest {
  providerId: string;
  overrides?: Record<string, string> | null;
}

export interface TriggerResultDto {
  correlationId: string;
  leadId: string;
}

export type FailureMode = 'Off' | 'FailOnce' | 'AlwaysFail';
export type ConsumerId = 'Customer' | 'Communications';

export interface SetFailureModeRequest {
  mode: FailureMode;
}

export interface IntegrationDetailsDto {
  messagingMode: string;
  providerName: string;
  idempotencyEnabled: boolean;
  retryMaxAttempts: number;
  backoffStrategy: string;
}

// Server-Sent Events payloads (GET /api/v1/events).
export type MicroserviceId = 'SalesLeads' | 'Customer' | 'Communications';
export type FlowStepKind =
  | 'Published'
  | 'Delivered'
  | 'Consumed'
  | 'CustomerCreated'
  | 'EmailSent'
  | 'Retrying'
  | 'DeadLettered'
  | 'Failed';

export interface FlowStepDto {
  sequence: number;
  service: MicroserviceId;
  kind: FlowStepKind;
  correlationId: string | null;
  leadId: string | null;
  occurredAtUtc: string;
  attempt: number | null;
  failureKind: string | null;
  reason: string | null;
  completed: boolean;
}

export interface CountersDto {
  published: number;
  customersCreated: number;
  emailsSent: number;
}
