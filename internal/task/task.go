package task

func (c *ConditionEndpointSuccess) Evaluate() (bool, error) {
	return false, nil
}

func (c *ConditionEndpointValue) Evaluate() (bool, error) {
	return false, nil
}

func (c *ConditionEndpointSuccess) GetType() ConditionType {
	return ConditionTypeEndpointSuccess
}

func (c *ConditionEndpointValue) GetType() ConditionType {
	return ConditionTypeEndpointValue
}

func (a *ActionEndpoint) Execute() error {
	return nil
}

func (a *ActionEndpoint) GetType() ActionType {
	return ActionTypeEndpoint
}
