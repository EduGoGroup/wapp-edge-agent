module github.com/EduGoGroup/wapp-edge-agent

go 1.25

require (
	github.com/EduGoGroup/wapp-shared/config v0.0.0-00010101000000-000000000000
	github.com/EduGoGroup/wapp-shared/logger v0.0.0-00010101000000-000000000000
)

require gopkg.in/yaml.v3 v3.0.1 // indirect

replace github.com/EduGoGroup/wapp-shared/logger => ../../shared/wapp-shared/logger

replace github.com/EduGoGroup/wapp-shared/config => ../../shared/wapp-shared/config

replace github.com/EduGoGroup/wapp-shared/envelope => ../../shared/wapp-shared/envelope
