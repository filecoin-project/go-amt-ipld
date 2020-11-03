package amt

import (
	"fmt"
)

var defaultBitWidth = 3

type config struct {
	bitWidth uint
}

type Option func(*config) error

func UseTreeBitWidth(bitWidth int) Option {
	return func(c *config) error {
		if bitWidth < 1 {
			return fmt.Errorf("bit width must be at least 2, is %d", bitWidth)
		}
		c.bitWidth = uint(bitWidth)
		return nil
	}
}

func defaultConfig() *config {
	return &config{
		bitWidth: uint(defaultBitWidth),
	}
}
