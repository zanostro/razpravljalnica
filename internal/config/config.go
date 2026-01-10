package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type NodeRole string

const (
	RoleHead   NodeRole = "HEAD"
	RoleMiddle NodeRole = "MIDDLE"
	RoleTail   NodeRole = "TAIL"
)

type NodeConfig struct {
	NodeID  string   `yaml:"node_id"`
	Address string   `yaml:"address"`
	Role    NodeRole `yaml:"role"`
}

type ChainConfig struct {
	Nodes []NodeConfig `yaml:"nodes"`
}

type Config struct {
	Chain ChainConfig `yaml:"chain"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return &cfg, nil
}

func (c *Config) Validate() error {
	if len(c.Chain.Nodes) == 0 {
		return fmt.Errorf("no nodes defined")
	}

	var hasHead, hasTail bool
	for _, n := range c.Chain.Nodes {
		if n.NodeID == "" {
			return fmt.Errorf("node without node_id")
		}
		if n.Address == "" {
			return fmt.Errorf("node %s without address", n.NodeID)
		}
		if n.Role == RoleHead {
			hasHead = true
		}
		if n.Role == RoleTail {
			hasTail = true
		}
	}

	if !hasHead {
		return fmt.Errorf("no HEAD node defined")
	}
	if !hasTail {
		return fmt.Errorf("no TAIL node defined")
	}

	return nil
}

// GetNode finds a node by ID
func (c *Config) GetNode(nodeID string) *NodeConfig {
	for i := range c.Chain.Nodes {
		if c.Chain.Nodes[i].NodeID == nodeID {
			return &c.Chain.Nodes[i]
		}
	}
	return nil
}

// GetHead returns the HEAD node
func (c *Config) GetHead() *NodeConfig {
	for i := range c.Chain.Nodes {
		if c.Chain.Nodes[i].Role == RoleHead {
			return &c.Chain.Nodes[i]
		}
	}
	return nil
}

// GetTail returns the TAIL node
func (c *Config) GetTail() *NodeConfig {
	for i := range c.Chain.Nodes {
		if c.Chain.Nodes[i].Role == RoleTail {
			return &c.Chain.Nodes[i]
		}
	}
	return nil
}

// GetSuccessor returns the next node in the chain after the given nodeID
func (c *Config) GetSuccessor(nodeID string) *NodeConfig {
	for i, n := range c.Chain.Nodes {
		if n.NodeID == nodeID && i < len(c.Chain.Nodes)-1 {
			return &c.Chain.Nodes[i+1]
		}
	}
	return nil
}

// GetPredecessor returns the previous node in the chain before the given nodeID
func (c *Config) GetPredecessor(nodeID string) *NodeConfig {
	for i, n := range c.Chain.Nodes {
		if n.NodeID == nodeID && i > 0 {
			return &c.Chain.Nodes[i-1]
		}
	}
	return nil
}
