package amt

import (
	"fmt"
)

var defaultBitWidth = 8

type config struct {
	width int
}

type Option func(*config) error

func UseTreeBitWidth(bitWidth int) Option {
	return func(c *config) error {
		if bitWidth < MinBitWidth {
			return fmt.Errorf("bit width must be at least 2, is %d", bitWidth)
		}
		c.width = bitWidth
		return nil
	}
}

func defaultConfig() *config {
	return &config{
		width: defaultBitWidth,
	}
}
