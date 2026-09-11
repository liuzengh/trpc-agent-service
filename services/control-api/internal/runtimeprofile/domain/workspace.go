package domain

func validateExecutorResource(key, p string, o map[string]any, d *[]Diagnostic) {
	validateAllowedFields(o, p, []string{"kind"}, d)
	requireFields(o, p, []string{"kind"}, d)
	if v, ok := o["kind"]; ok && v != "sdk_sandbox" {
		*d = append(*d, errorDiagnostic("RUNTIME_PROFILE_SPEC_EXECUTOR_KIND_UNSUPPORTED", p+"/kind", "executor kind must be sdk_sandbox"))
	}
}
