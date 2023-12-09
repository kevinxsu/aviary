#!/bin/bash
(cd main && go run aviaryworker.go) & 
(cd main && go run aviaryworker.go) & 
(cd main && go run aviaryworker.go) & 
(cd main && go run aviarycoordinator.go) & 
(cd main && go run clerk.go) && fg

# echo "hello" 
