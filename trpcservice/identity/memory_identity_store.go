package identity

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

type memoryLoginProvider struct {
	descriptor   ProviderDescriptor
	enterpriseID string
}

type memoryTenant struct {
	displayName string
	status      string
}

type MemoryIdentityStore struct {
	mu               sync.Mutex
	users            map[string]PlatformUser
	providers        map[string]memoryLoginProvider
	logins           map[string]LoginIdentity
	systemAdmins     map[string]bool
	tenants          map[string]memoryTenant
	tenantModels     map[string][]TenantModelGrant
	tenantTools      map[string][]TenantToolGrant
	memberships      map[string]TenantMembership
	channels         map[string]ChannelIdentity
	localCredentials map[string]LocalCredential
	audits           []memoryAudit
}

type memoryAudit struct {
	Action string
	Result string
	Detail string
	At     time.Time
}

func NewMemoryIdentityStore() *MemoryIdentityStore {
	return &MemoryIdentityStore{
		users: make(map[string]PlatformUser), providers: make(map[string]memoryLoginProvider), logins: make(map[string]LoginIdentity),
		systemAdmins: make(map[string]bool),
		tenants:      make(map[string]memoryTenant), tenantModels: make(map[string][]TenantModelGrant), tenantTools: make(map[string][]TenantToolGrant), memberships: make(map[string]TenantMembership), channels: make(map[string]ChannelIdentity),
		localCredentials: make(map[string]LocalCredential),
	}
}

func loginKey(providerID, subjectID string) string     { return providerID + "\x00" + subjectID }
func memberKey(tenantID, platformUserID string) string { return tenantID + "\x00" + platformUserID }
func channelIdentityKey(tenantID string, channel channels.Channel, bindingID, externalUserID string) string {
	return strings.Join([]string{tenantID, string(channel), bindingID, externalUserID}, "\x00")
}

func (s *MemoryIdentityStore) UpsertLoginProvider(_ context.Context, descriptor ProviderDescriptor, enterpriseID string) error {
	enterpriseID = strings.TrimSpace(enterpriseID)
	if strings.TrimSpace(descriptor.ProviderID) == "" || strings.TrimSpace(descriptor.DisplayName) == "" || enterpriseID == "" {
		return errors.New("login provider metadata is incomplete")
	}
	if descriptor.Type != ProviderLocal && descriptor.Type != ProviderWeCom && descriptor.Type != ProviderFeishu && descriptor.Type != ProviderOIDC && descriptor.Type != ProviderMock {
		return errors.New("unsupported login provider type")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.providers[descriptor.ProviderID] = memoryLoginProvider{descriptor: descriptor, enterpriseID: enterpriseID}
	return nil
}

func (s *MemoryIdentityStore) CreateLocalUser(_ context.Context, username, displayName, email, passwordHash string, mustChangePassword bool) (PlatformUser, error) {
	username, err := NormalizeLocalUsername(username)
	if err != nil {
		return PlatformUser{}, err
	}
	if strings.TrimSpace(passwordHash) == "" {
		return PlatformUser{}, errors.New("password hash is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.localCredentials[username]; exists {
		return PlatformUser{}, ErrLocalUsernameTaken
	}
	if strings.TrimSpace(displayName) == "" {
		displayName = username
	}
	now := time.Now().UTC()
	user := PlatformUser{PlatformUserID: uuid.NewString(), DisplayName: strings.TrimSpace(displayName), Email: strings.TrimSpace(email), Status: "active", FirstSeenAt: now, LastLoginAt: now}
	s.users[user.PlatformUserID] = user
	s.providers["local"] = memoryLoginProvider{descriptor: ProviderDescriptor{ProviderID: "local", Type: ProviderLocal, DisplayName: "本地账号"}, enterpriseID: "local"}
	s.logins[loginKey("local", username)] = LoginIdentity{ProviderID: "local", SubjectID: username, PlatformUserID: user.PlatformUserID, DisplayName: user.DisplayName, Email: user.Email, LastLoginAt: now}
	s.localCredentials[username] = LocalCredential{PlatformUserID: user.PlatformUserID, Username: username, PasswordHash: passwordHash, MustChangePassword: mustChangePassword}
	return user, nil
}

func (s *MemoryIdentityStore) LookupLocalCredential(_ context.Context, username string) (LocalCredential, PlatformUser, error) {
	username, err := NormalizeLocalUsername(username)
	if err != nil {
		return LocalCredential{}, PlatformUser{}, ErrLocalCredentialNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	credential, ok := s.localCredentials[username]
	if !ok {
		return LocalCredential{}, PlatformUser{}, ErrLocalCredentialNotFound
	}
	user, ok := s.users[credential.PlatformUserID]
	if !ok {
		return LocalCredential{}, PlatformUser{}, ErrLocalCredentialNotFound
	}
	return credential, user, nil
}

func (s *MemoryIdentityStore) LocalCredentialForUser(_ context.Context, platformUserID string) (LocalCredential, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, credential := range s.localCredentials {
		if credential.PlatformUserID == strings.TrimSpace(platformUserID) {
			return credential, nil
		}
	}
	return LocalCredential{}, ErrLocalCredentialNotFound
}

func (s *MemoryIdentityStore) SetLocalPassword(_ context.Context, platformUserID, passwordHash string, mustChangePassword bool) error {
	if strings.TrimSpace(passwordHash) == "" {
		return errors.New("password hash is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for username, credential := range s.localCredentials {
		if credential.PlatformUserID != platformUserID {
			continue
		}
		credential.PasswordHash = passwordHash
		credential.MustChangePassword = mustChangePassword
		s.localCredentials[username] = credential
		return nil
	}
	return ErrLocalCredentialNotFound
}

func (s *MemoryIdentityStore) HasUsableSystemAdmin(_ context.Context) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.usableSystemAdminCountLocked("") > 0, nil
}

func (s *MemoryIdentityStore) ResolveSessionUser(_ context.Context, platformUserID string) (SessionUser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	user, ok := s.users[strings.TrimSpace(platformUserID)]
	if !ok {
		return SessionUser{}, ErrPlatformUserNotFound
	}
	if user.Status != "active" {
		return SessionUser{}, ErrPlatformUserSuspended
	}
	principal := SessionUser{PlatformUserID: user.PlatformUserID, DisplayName: user.DisplayName, Email: user.Email, IsSystemAdmin: s.systemAdmins[user.PlatformUserID], Tenants: make([]TenantRole, 0)}
	for _, credential := range s.localCredentials {
		if credential.PlatformUserID == user.PlatformUserID {
			principal.MustChangePassword = credential.MustChangePassword
			break
		}
	}
	for tenantID, tenant := range s.tenants {
		membership, exists := s.memberships[memberKey(tenantID, user.PlatformUserID)]
		if !exists || membership.Status != "active" {
			continue
		}
		principal.Tenants = append(principal.Tenants, TenantRole{
			TenantID: tenantID, DisplayName: tenant.displayName, Role: membership.Role, Status: membership.Status,
			ConversationContentAudit: membership.ConversationContentAudit,
		})
	}
	sort.Slice(principal.Tenants, func(i, j int) bool { return principal.Tenants[i].TenantID < principal.Tenants[j].TenantID })
	if len(principal.Tenants) == 1 {
		principal.Role = principal.Tenants[0].Role
	}
	return principal, nil
}

func (s *MemoryIdentityStore) ResolveLoginIdentity(_ context.Context, external Identity) (PlatformUser, error) {
	if err := validateExternalIdentity(external); err != nil {
		return PlatformUser{}, err
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	provider, ok := s.providers[external.ProviderID]
	if !ok || provider.descriptor.Type != external.ProviderType || provider.enterpriseID != external.EnterpriseID {
		return PlatformUser{}, errors.New("login identity does not match the registered provider boundary")
	}
	key := loginKey(external.ProviderID, external.SubjectID)
	if login, ok := s.logins[key]; ok {
		user := s.users[login.PlatformUserID]
		if strings.TrimSpace(external.Email) != "" {
			user.Email = strings.TrimSpace(external.Email)
		}
		user.LastLoginAt = now
		s.users[user.PlatformUserID] = user
		if strings.TrimSpace(external.DisplayName) != "" {
			login.DisplayName = strings.TrimSpace(external.DisplayName)
		}
		login.Email, login.LastLoginAt = user.Email, now
		s.logins[key] = login
		return user, nil
	}
	user := PlatformUser{PlatformUserID: uuid.NewString(), DisplayName: strings.TrimSpace(external.DisplayName), Email: strings.TrimSpace(external.Email), Status: "active", FirstSeenAt: now, LastLoginAt: now}
	s.users[user.PlatformUserID] = user
	s.logins[key] = LoginIdentity{ProviderID: external.ProviderID, SubjectID: external.SubjectID, PlatformUserID: user.PlatformUserID, DisplayName: user.DisplayName, Email: user.Email, LastLoginAt: now}
	return user, nil
}

func (s *MemoryIdentityStore) LookupLoginIdentity(_ context.Context, providerID, subjectID string) (LoginIdentity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	login, ok := s.logins[loginKey(strings.TrimSpace(providerID), strings.TrimSpace(subjectID))]
	if !ok {
		return LoginIdentity{}, ErrPlatformUserNotFound
	}
	return login, nil
}

func (s *MemoryIdentityStore) LinkLoginIdentity(_ context.Context, platformUserID string, external Identity) error {
	if external.ProviderType == ProviderLocal {
		return errors.New("local login identities are provisioned by administrators")
	}
	if err := validateExternalIdentity(external); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	user, ok := s.users[strings.TrimSpace(platformUserID)]
	if !ok || user.Status != "active" {
		return ErrPlatformUserNotFound
	}
	provider, ok := s.providers[external.ProviderID]
	if !ok || provider.descriptor.Type != external.ProviderType || provider.enterpriseID != external.EnterpriseID {
		return errors.New("login identity does not match the registered provider boundary")
	}
	key := loginKey(external.ProviderID, external.SubjectID)
	if existing, exists := s.logins[key]; exists {
		if existing.PlatformUserID != platformUserID {
			return ErrLoginIdentityConflict
		}
		existing.DisplayName = firstNonEmpty(strings.TrimSpace(external.DisplayName), existing.DisplayName)
		existing.Email = firstNonEmpty(strings.TrimSpace(external.Email), existing.Email)
		existing.LastLoginAt = time.Now().UTC()
		s.logins[key] = existing
		return nil
	}
	now := time.Now().UTC()
	s.logins[key] = LoginIdentity{ProviderID: external.ProviderID, SubjectID: external.SubjectID, PlatformUserID: platformUserID, DisplayName: strings.TrimSpace(external.DisplayName), Email: strings.TrimSpace(external.Email), LastLoginAt: now}
	return nil
}

func firstNonEmpty(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func (s *MemoryIdentityStore) ListLoginMethods(_ context.Context, platformUserID string) ([]LoginMethod, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	methods := make([]LoginMethod, 0)
	for _, login := range s.logins {
		if login.PlatformUserID != platformUserID {
			continue
		}
		provider := s.providers[login.ProviderID]
		methods = append(methods, LoginMethod{ProviderID: login.ProviderID, ProviderType: provider.descriptor.Type, DisplayName: provider.descriptor.DisplayName, SubjectID: login.SubjectID, LinkedAt: login.LastLoginAt})
	}
	sort.Slice(methods, func(i, j int) bool {
		if methods[i].ProviderID == methods[j].ProviderID {
			return methods[i].SubjectID < methods[j].SubjectID
		}
		return methods[i].ProviderID < methods[j].ProviderID
	})
	return methods, nil
}

func (s *MemoryIdentityStore) LatestLoginAtForProvider(_ context.Context, providerID string) (time.Time, bool, error) {
	providerID = strings.TrimSpace(providerID)
	if providerID == "" {
		return time.Time{}, false, errors.New("login provider ID is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var latest time.Time
	for _, login := range s.logins {
		if login.ProviderID != providerID || login.LastLoginAt.IsZero() {
			continue
		}
		if latest.IsZero() || login.LastLoginAt.After(latest) {
			latest = login.LastLoginAt
		}
	}
	if latest.IsZero() {
		return time.Time{}, false, nil
	}
	return latest.UTC(), true, nil
}

func (s *MemoryIdentityStore) RemoveLoginIdentity(_ context.Context, platformUserID, providerID, subjectID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, login := range s.logins {
		if login.PlatformUserID == platformUserID {
			count++
		}
	}
	if count <= 1 {
		return ErrLastLoginIdentity
	}
	key := loginKey(strings.TrimSpace(providerID), strings.TrimSpace(subjectID))
	login, ok := s.logins[key]
	if !ok || login.PlatformUserID != platformUserID {
		return ErrPlatformUserNotFound
	}
	delete(s.logins, key)
	if providerID == "local" {
		for username, credential := range s.localCredentials {
			if credential.PlatformUserID == platformUserID {
				delete(s.localCredentials, username)
			}
		}
	}
	return nil
}

func (s *MemoryIdentityStore) SetSystemAdmin(_ context.Context, platformUserID string, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	user, ok := s.users[platformUserID]
	if !ok {
		return ErrPlatformUserNotFound
	}
	if enabled {
		if user.Status != "active" {
			return errors.New("a suspended user cannot be a system administrator")
		}
		s.systemAdmins[platformUserID] = true
	} else {
		if s.systemAdmins[platformUserID] && s.usableSystemAdminCountLocked(platformUserID) == 0 {
			return ErrLastSystemAdmin
		}
		delete(s.systemAdmins, platformUserID)
	}
	return nil
}

func (s *MemoryIdentityStore) IsSystemAdmin(_ context.Context, platformUserID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.systemAdmins[platformUserID], nil
}

func (s *MemoryIdentityStore) ListPlatformUsers(_ context.Context, request MemberPageRequest) (MemberPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]MemberSummary, 0)
	for _, user := range s.users {
		result = append(result, s.memberSummaryLocked(user, "", user.Status))
	}
	return paginateMemberSummaries(result, request), nil
}

func (s *MemoryIdentityStore) UpdatePlatformUserProfile(_ context.Context, platformUserID, displayName string) error {
	platformUserID, displayName = strings.TrimSpace(platformUserID), strings.TrimSpace(displayName)
	if displayName == "" || len([]rune(displayName)) > 80 {
		return errors.New("display name must be between 1 and 80 characters")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	user, ok := s.users[platformUserID]
	if !ok {
		return ErrPlatformUserNotFound
	}
	user.DisplayName = displayName
	s.users[platformUserID] = user
	return nil
}

func (s *MemoryIdentityStore) UpdatePlatformUserAccess(_ context.Context, platformUserID, status string, systemAdmin bool) error {
	platformUserID, status = strings.TrimSpace(platformUserID), strings.TrimSpace(status)
	if status != "active" && status != "suspended" {
		return errors.New("invalid platform user status")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	user, ok := s.users[platformUserID]
	if !ok {
		return ErrPlatformUserNotFound
	}
	if status == "suspended" {
		if s.systemAdmins[platformUserID] && s.usableSystemAdminCountLocked(platformUserID) == 0 {
			return ErrLastSystemAdmin
		}
		if s.isLastActiveTenantAdminLocked(platformUserID) {
			return ErrLastTenantAdmin
		}
	}
	if systemAdmin && status != "active" {
		return errors.New("a suspended user cannot be a system administrator")
	}
	if !systemAdmin && s.systemAdmins[platformUserID] && s.usableSystemAdminCountLocked(platformUserID) == 0 {
		return ErrLastSystemAdmin
	}
	user.Status = status
	s.users[platformUserID] = user
	if systemAdmin {
		s.systemAdmins[platformUserID] = true
	} else {
		delete(s.systemAdmins, platformUserID)
	}
	return nil
}

func (s *MemoryIdentityStore) memberSummaryLocked(user PlatformUser, role, status string) MemberSummary {
	providers := make([]string, 0)
	seen := map[string]bool{}
	for _, login := range s.logins {
		if login.PlatformUserID != user.PlatformUserID {
			continue
		}
		if provider, ok := s.providers[login.ProviderID]; ok && !seen[provider.descriptor.DisplayName] {
			providers = append(providers, provider.descriptor.DisplayName)
			seen[provider.descriptor.DisplayName] = true
		}
	}
	sort.Strings(providers)
	return MemberSummary{PlatformUserID: user.PlatformUserID, DisplayName: user.DisplayName, Email: user.Email, Role: role, Status: status, IsSystemAdmin: s.systemAdmins[user.PlatformUserID], LastLoginAt: user.LastLoginAt, Providers: providers}
}

func (s *MemoryIdentityStore) CreateTenant(_ context.Context, tenantID, displayName, initialAdminPlatformUserID string) error {
	tenantID, displayName, initialAdminPlatformUserID = strings.TrimSpace(tenantID), strings.TrimSpace(displayName), strings.TrimSpace(initialAdminPlatformUserID)
	if tenantID == "" || displayName == "" || initialAdminPlatformUserID == "" {
		return errors.New("tenant ID, display name and initial administrator are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tenants[tenantID]; exists {
		return errors.New("tenant already exists")
	}
	user, ok := s.users[initialAdminPlatformUserID]
	if !ok {
		return ErrPlatformUserNotFound
	}
	if user.Status != "active" {
		return errors.New("initial tenant administrator must be active")
	}
	s.tenants[tenantID] = memoryTenant{displayName: displayName, status: "active"}
	s.memberships[memberKey(tenantID, initialAdminPlatformUserID)] = TenantMembership{TenantID: tenantID, PlatformUserID: initialAdminPlatformUserID, Role: RoleAdmin, Status: "active"}
	return nil
}

func (s *MemoryIdentityStore) ListTenantModelGrants(_ context.Context, tenantID string) ([]TenantModelGrant, error) {
	tenantID = strings.TrimSpace(tenantID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tenants[tenantID]; !exists {
		return nil, ErrTenantNotFound
	}
	grants := append([]TenantModelGrant(nil), s.tenantModels[tenantID]...)
	sort.Slice(grants, func(i, j int) bool {
		if grants[i].ProviderID == grants[j].ProviderID {
			return grants[i].ModelName < grants[j].ModelName
		}
		return grants[i].ProviderID < grants[j].ProviderID
	})
	return grants, nil
}

func (s *MemoryIdentityStore) ReplaceTenantModelGrants(_ context.Context, tenantID string, grants []TenantModelGrant) error {
	tenantID = strings.TrimSpace(tenantID)
	normalized, err := normalizeTenantModelGrants(grants)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tenants[tenantID]; !exists {
		return ErrTenantNotFound
	}
	s.tenantModels[tenantID] = append([]TenantModelGrant(nil), normalized...)
	return nil
}

func (s *MemoryIdentityStore) ListTenantToolGrants(_ context.Context, tenantID string) ([]TenantToolGrant, error) {
	tenantID = strings.TrimSpace(tenantID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tenants[tenantID]; !exists {
		return nil, ErrTenantNotFound
	}
	grants := append([]TenantToolGrant(nil), s.tenantTools[tenantID]...)
	sort.Slice(grants, func(i, j int) bool { return grants[i].ToolName < grants[j].ToolName })
	return grants, nil
}

func (s *MemoryIdentityStore) ReplaceTenantToolGrants(_ context.Context, tenantID string, grants []TenantToolGrant) error {
	tenantID = strings.TrimSpace(tenantID)
	normalized, err := normalizeTenantToolGrants(grants)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tenants[tenantID]; !exists {
		return ErrTenantNotFound
	}
	s.tenantTools[tenantID] = append([]TenantToolGrant(nil), normalized...)
	return nil
}

func (s *MemoryIdentityStore) TenantStatus(_ context.Context, tenantID string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tenant, ok := s.tenants[strings.TrimSpace(tenantID)]
	if !ok {
		return "", ErrTenantNotFound
	}
	return tenant.status, nil
}

func (s *MemoryIdentityStore) SetTenantStatus(_ context.Context, tenantID, status string) error {
	tenantID, status = strings.TrimSpace(tenantID), strings.TrimSpace(status)
	if tenantID == "" || (status != TenantActive && status != TenantSuspended) {
		return errors.New("invalid tenant status")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tenant, ok := s.tenants[tenantID]
	if !ok {
		return ErrTenantNotFound
	}
	if status == TenantActive && s.activeTenantAdminCountLocked(tenantID, "") == 0 {
		return ErrTenantNeedsAdmin
	}
	tenant.status = status
	s.tenants[tenantID] = tenant
	return nil
}

// UpsertTenant remains a test/setup convenience. Product code creates tenants
// through CreateTenant.
func (s *MemoryIdentityStore) UpsertTenant(_ context.Context, tenantID, displayName string) error {
	tenantID, displayName = strings.TrimSpace(tenantID), strings.TrimSpace(displayName)
	if tenantID == "" || displayName == "" {
		return errors.New("tenant ID and display name are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tenants[tenantID] = memoryTenant{displayName: displayName, status: "active"}
	return nil
}

func (s *MemoryIdentityStore) GrantMembership(ctx context.Context, tenantID, platformUserID string, role Role) error {
	return s.SetTenantMembership(ctx, tenantID, platformUserID, role, "active")
}

func (s *MemoryIdentityStore) SetTenantMembership(_ context.Context, tenantID, platformUserID string, role Role, status string) error {
	if role != RoleAdmin && role != RoleMember {
		return errors.New("invalid tenant role")
	}
	if status != "active" && status != "suspended" {
		return errors.New("invalid tenant membership status")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	user, ok := s.users[platformUserID]
	if !ok {
		return ErrPlatformUserNotFound
	}
	if status == "active" && user.Status != "active" {
		return errors.New("a suspended platform user cannot have an active tenant membership")
	}
	if _, ok := s.tenants[tenantID]; !ok {
		s.tenants[tenantID] = memoryTenant{displayName: tenantID, status: "active"}
	}
	key := memberKey(tenantID, platformUserID)
	previous, hadPrevious := s.memberships[key]
	if hadPrevious && previous.Role == RoleAdmin && previous.Status == "active" && (role != RoleAdmin || status != "active") {
		tenant := s.tenants[tenantID]
		if tenant.status == "active" && s.activeTenantAdminCountLocked(tenantID, platformUserID) == 0 {
			return ErrLastTenantAdmin
		}
	}
	auditPermission := previous.ConversationContentAudit
	if role != RoleAdmin || status != "active" {
		auditPermission = false
	}
	s.memberships[key] = TenantMembership{
		TenantID: tenantID, PlatformUserID: platformUserID, Role: role, Status: status,
		ConversationContentAudit: auditPermission,
	}
	return nil
}

func (s *MemoryIdentityStore) SetConversationContentAudit(_ context.Context, tenantID, platformUserID string, enabled bool) error {
	tenantID = strings.TrimSpace(tenantID)
	platformUserID = strings.TrimSpace(platformUserID)
	if tenantID == "" || platformUserID == "" {
		return errors.New("tenant and platform user are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := memberKey(tenantID, platformUserID)
	membership, ok := s.memberships[key]
	if !ok || membership.Role != RoleAdmin || membership.Status != "active" {
		return errors.New("conversation content audit requires an active tenant administrator")
	}
	membership.ConversationContentAudit = enabled
	s.memberships[key] = membership
	return nil
}

func (s *MemoryIdentityStore) usableSystemAdminCountLocked(excludePlatformUserID string) int {
	count := 0
	for platformUserID := range s.systemAdmins {
		if platformUserID == excludePlatformUserID {
			continue
		}
		if user, ok := s.users[platformUserID]; ok && user.Status == "active" {
			count++
		}
	}
	return count
}

func (s *MemoryIdentityStore) activeTenantAdminCountLocked(tenantID, excludePlatformUserID string) int {
	count := 0
	for _, membership := range s.memberships {
		if membership.TenantID != tenantID || membership.PlatformUserID == excludePlatformUserID || membership.Role != RoleAdmin || membership.Status != "active" {
			continue
		}
		if user, ok := s.users[membership.PlatformUserID]; ok && user.Status == "active" {
			count++
		}
	}
	return count
}

func (s *MemoryIdentityStore) isLastActiveTenantAdminLocked(platformUserID string) bool {
	for _, membership := range s.memberships {
		if membership.PlatformUserID != platformUserID || membership.Role != RoleAdmin || membership.Status != "active" {
			continue
		}
		if tenant, ok := s.tenants[membership.TenantID]; ok && tenant.status == "active" && s.activeTenantAdminCountLocked(membership.TenantID, platformUserID) == 0 {
			return true
		}
	}
	return false
}

func (s *MemoryIdentityStore) ListTenantMemberships(_ context.Context, platformUserID string) ([]TenantMembership, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]TenantMembership, 0)
	for _, membership := range s.memberships {
		if membership.PlatformUserID == platformUserID {
			result = append(result, membership)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].TenantID < result[j].TenantID })
	return result, nil
}

func (s *MemoryIdentityStore) ListTenantMembers(_ context.Context, tenantID string, request MemberPageRequest) (MemberPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]MemberSummary, 0)
	for _, membership := range s.memberships {
		if membership.TenantID != tenantID {
			continue
		}
		item := s.memberSummaryLocked(s.users[membership.PlatformUserID], string(membership.Role), membership.Status)
		item.ConversationContentAudit = membership.ConversationContentAudit
		result = append(result, item)
	}
	return paginateMemberSummaries(result, request), nil
}

func (s *MemoryIdentityStore) ListTenantMemberCandidates(_ context.Context, tenantID string, request MemberPageRequest) (MemberPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]MemberSummary, 0)
	for _, user := range s.users {
		if user.Status != "active" {
			continue
		}
		if _, exists := s.memberships[memberKey(tenantID, user.PlatformUserID)]; exists {
			continue
		}
		result = append(result, s.memberSummaryLocked(user, "", user.Status))
	}
	return paginateMemberSummaries(result, request), nil
}

func paginateMemberSummaries(items []MemberSummary, request MemberPageRequest) MemberPage {
	request = normalizeMemberPageRequest(request)
	query := strings.ToLower(request.Query)
	filtered := items[:0]
	for _, item := range items {
		if query != "" {
			matched := strings.Contains(strings.ToLower(item.PlatformUserID), query) ||
				strings.Contains(strings.ToLower(item.DisplayName), query) ||
				strings.Contains(strings.ToLower(item.Email), query)
			if !matched {
				continue
			}
		}
		if request.After != nil {
			if item.DisplayName < request.After.DisplayName || (item.DisplayName == request.After.DisplayName && item.PlatformUserID <= request.After.PlatformUserID) {
				continue
			}
		}
		filtered = append(filtered, item)
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].DisplayName == filtered[j].DisplayName {
			return filtered[i].PlatformUserID < filtered[j].PlatformUserID
		}
		return filtered[i].DisplayName < filtered[j].DisplayName
	})
	page := MemberPage{Members: filtered}
	if len(page.Members) > request.Limit {
		page.Members = page.Members[:request.Limit]
		last := page.Members[len(page.Members)-1]
		page.Next = &MemberCursor{DisplayName: last.DisplayName, PlatformUserID: last.PlatformUserID}
	}
	return page
}

func (s *MemoryIdentityStore) RoleFor(_ context.Context, tenantID, platformUserID string) (Role, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	membership, ok := s.memberships[memberKey(tenantID, platformUserID)]
	if !ok || membership.Status != "active" {
		return "", errors.New("not an active tenant member")
	}
	return membership.Role, nil
}

func (s *MemoryIdentityStore) ListTenantSummaries(_ context.Context, platformUserID string, includeAll bool) ([]TenantSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]TenantSummary, 0)
	for tenantID, tenant := range s.tenants {
		membership, ok := s.memberships[memberKey(tenantID, platformUserID)]
		if !includeAll && (!ok || membership.Status != "active") {
			continue
		}
		role := Role("")
		if ok && membership.Status == "active" {
			role = membership.Role
		}
		result = append(result, TenantSummary{TenantID: tenantID, DisplayName: tenant.displayName, Role: role, Status: tenant.status})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].DisplayName == result[j].DisplayName {
			return result[i].TenantID < result[j].TenantID
		}
		return result[i].DisplayName < result[j].DisplayName
	})
	return result, nil
}

func (s *MemoryIdentityStore) ResolveChannelIdentity(_ context.Context, tenantID string, channel channels.Channel, bindingID, externalUserID, trustedEnterpriseID string) (ChannelIdentity, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	base := ChannelIdentity{TenantID: tenantID, Channel: channel, BindingID: bindingID, ExternalUserID: externalUserID, TrustedEnterpriseID: strings.TrimSpace(trustedEnterpriseID)}
	key := channelIdentityKey(tenantID, channel, bindingID, externalUserID)
	if linked, ok := s.channels[key]; ok && linked.PlatformUserID != "" {
		return linked, true, nil
	}
	if _, ok := s.channels[key]; !ok {
		s.channels[key] = base
	}
	providerType, trustedChannel := trustedLoginProviderType(channel)
	if !trustedChannel || strings.TrimSpace(trustedEnterpriseID) == "" {
		return base, false, nil
	}
	var linkedPlatformUserID string
	for _, existing := range s.channels {
		if existing.TenantID != tenantID || existing.Channel != channel || existing.ExternalUserID != externalUserID || existing.PlatformUserID == "" || existing.TrustedEnterpriseID != strings.TrimSpace(trustedEnterpriseID) {
			continue
		}
		if linkedPlatformUserID != "" && linkedPlatformUserID != existing.PlatformUserID {
			return ChannelIdentity{}, false, ErrChannelIdentityConflict
		}
		linkedPlatformUserID = existing.PlatformUserID
	}
	if linkedPlatformUserID != "" {
		membership, ok := s.memberships[memberKey(tenantID, linkedPlatformUserID)]
		user, userOK := s.users[linkedPlatformUserID]
		if ok && membership.Status == "active" && userOK && user.Status == "active" {
			base.PlatformUserID = linkedPlatformUserID
			base.LinkedAt = time.Now().UTC()
			s.channels[key] = base
			return base, true, nil
		}
	}
	for providerID, provider := range s.providers {
		if provider.descriptor.Type != providerType || provider.enterpriseID != trustedEnterpriseID {
			continue
		}
		login, ok := s.logins[loginKey(providerID, externalUserID)]
		if !ok {
			continue
		}
		membership, ok := s.memberships[memberKey(tenantID, login.PlatformUserID)]
		if !ok || membership.Status != "active" || s.users[login.PlatformUserID].Status != "active" {
			continue
		}
		base.PlatformUserID = login.PlatformUserID
		base.LinkedAt = time.Now().UTC()
		s.channels[key] = base
		return base, true, nil
	}
	return base, false, nil
}

func (s *MemoryIdentityStore) RecordAudit(_ context.Context, action, result, detail string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.audits = append(s.audits, memoryAudit{Action: action, Result: result, Detail: detail, At: time.Now().UTC()})
	return nil
}

func (s *MemoryIdentityStore) Audits() []memoryAudit {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]memoryAudit(nil), s.audits...)
}

var _ LoginProviderRegistrar = (*MemoryIdentityStore)(nil)
var _ AuthIdentityStore = (*MemoryIdentityStore)(nil)
var _ LocalBootstrapStore = (*MemoryIdentityStore)(nil)
var _ ConsoleIdentityStore = (*MemoryIdentityStore)(nil)
