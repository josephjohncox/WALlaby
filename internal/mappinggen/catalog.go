package mappinggen

import (
	"context"

	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
)

type CatalogScope = pgsource.CatalogScope
type CatalogTable = pgsource.CatalogTable
type CatalogColumn = pgsource.CatalogColumn

func InspectPostgres(ctx context.Context, dsn string, options map[string]string, scope CatalogScope) ([]CatalogTable, error) {
	return pgsource.InspectCatalog(ctx, dsn, options, scope)
}
