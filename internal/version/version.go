// Package version 保存构建时注入的版本信息。
package version

// 由构建参数 -ldflags "-X .../internal/version.Version=v1.0.0 ..." 注入。
var (
	Version = "dev"
	Commit  = "unknown"
	Date    = "unknown"
)
