package main

import (
	"context"
	"log"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
)

var version = "dev"

func main() {
	opts := providerserver.ServeOpts{
		Address: "registry.terraform.io/josephjohncox/wallaby",
	}
	if err := providerserver.Serve(context.Background(), New(version), opts); err != nil {
		log.Fatal(err)
	}
}
