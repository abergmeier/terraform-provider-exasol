package exaprovider

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

type CreateFunc func(*schema.ResourceData, *Connection) error
type CreateContextFunc func(context.Context, *schema.ResourceData, *Connection) diag.Diagnostics
type ReadContextFunc func(context.Context, *schema.ResourceData, *Connection) diag.Diagnostics
type UpdateFunc func(*schema.ResourceData, *Connection) error
type UpdateContextFunc func(context.Context, *schema.ResourceData, *Connection) diag.Diagnostics
type DeleteFunc func(*schema.ResourceData, *Connection) error
type DeleteContextFunc func(context.Context, *schema.ResourceData, *Connection) diag.Diagnostics
type ExistsFunc func(*schema.ResourceData, *Connection) (bool, error)
type ImportStateFunc func(*schema.ResourceData, *Connection) ([]*schema.ResourceData, error)
type ImportStateContextFunc func(context.Context, *schema.ResourceData, *Connection) ([]*schema.ResourceData, error)
