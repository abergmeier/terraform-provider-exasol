package cached

import (
	"context"

	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func Create(create exaprovider.CreateFunc, d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	conn, err := c.OpenManualConnection()
	if err != nil {
		return err
	}

	defer conn.Close()

	err = create(d, conn)
	if conn.WS != nil {
		conn.WS.Invalidate(d.Id())
	}
	return err
}

func CreateContext(create exaprovider.CreateContextFunc, ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	conn, err := c.OpenManualConnection()
	if err != nil {
		return diag.FromErr(err)
	}

	defer conn.Close()

	diags := create(ctx, d, conn)
	if conn.WS != nil {
		conn.WS.Invalidate(d.Id())
	}
	return diags
}

func ReadContext(read exaprovider.ReadContextFunc, ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	conn, err := c.OpenManualConnection()
	if err != nil {
		return diag.FromErr(err)
	}

	defer conn.Close()

	if conn.WS != nil {
		rec := conn.WS.Record(d.Id())
		defer rec.Close()
	}
	return read(ctx, d, conn)
}

func Update(update exaprovider.UpdateFunc, d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	conn, err := c.OpenManualConnection()
	if err != nil {
		return err
	}

	defer conn.Close()

	if conn.WS != nil {
		conn.WS.Invalidate(d.Id())
	}

	return update(d, conn)
}

func UpdateContext(update exaprovider.UpdateContextFunc, ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	conn, err := c.OpenManualConnection()
	if err != nil {
		return diag.FromErr(err)
	}

	defer conn.Close()

	if conn.WS != nil {
		conn.WS.Invalidate(d.Id())
	}

	return update(ctx, d, conn)
}

func Delete(delete exaprovider.DeleteFunc, d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	conn, err := c.OpenManualConnection()
	if err != nil {
		return err
	}

	defer conn.Close()

	if conn.WS != nil {
		conn.WS.Invalidate(d.Id())
	}

	return delete(d, conn)
}

func DeleteContext(delete exaprovider.DeleteContextFunc, ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)

	conn, err := c.OpenManualConnection()
	if err != nil {
		return diag.FromErr(err)
	}

	defer conn.Close()

	if conn.WS != nil {
		conn.WS.Invalidate(d.Id())
	}

	return delete(ctx, d, conn)
}

func Exists(exists exaprovider.ExistsFunc, d *schema.ResourceData, meta interface{}) (bool, error) {
	c := meta.(*exaprovider.Client)

	conn, err := c.OpenManualConnection()
	if err != nil {
		return false, err
	}

	defer conn.Close()

	if conn.WS != nil {
		rec := conn.WS.Record(d.Id())
		defer rec.Close()
	}
	return exists(d, conn)
}

func ImporterState(importState exaprovider.ImportStateFunc, d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	c := meta.(*exaprovider.Client)

	conn, err := c.OpenManualConnection()
	if err != nil {
		return nil, err
	}

	defer conn.Close()

	rd, err := importState(d, conn)

	if conn.WS != nil {
		conn.WS.Invalidate(d.Id())
	}

	return rd, err
}

func ImporterStateContext(importState exaprovider.ImportStateContextFunc, ctx context.Context, d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	c := meta.(*exaprovider.Client)

	conn, err := c.OpenManualConnection()
	if err != nil {
		return nil, err
	}

	defer conn.Close()

	rd, err := importState(ctx, d, conn)

	if conn.WS != nil {
		conn.WS.Invalidate(d.Id())
	}

	return rd, err
}
