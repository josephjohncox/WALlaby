module github.com/josephjohncox/ductstream/terraform/provider

go 1.22

require (
	github.com/hashicorp/terraform-plugin-framework v1.8.0
	google.golang.org/grpc v1.64.0
	github.com/josephjohncox/ductstream v0.0.0
)

replace github.com/josephjohncox/ductstream => ../..
