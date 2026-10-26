module github.com/TwelfthKyomu/ruvo/cmd/ruvo

go 1.27

require (
	github.com/TwelfthKyomu/ruvo v0.6.1
	github.com/TwelfthKyomu/ruvo/flow v0.6.1
)

require golang.org/x/exp v0.0.0-20261014143641-f0251eb4aa3e // indirect

retract (
	[v0.1.2, v0.6.0] // Superseded; use the latest version.
	[v0.1.0, v0.1.1] // Published by mistake with files that were not meant to be public.
)
