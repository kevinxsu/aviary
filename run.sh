#!/bin/bash
(cd main && go run aviaryworker.go) & 
(cd main && go run aviaryworker.go) & 
(cd main && go run aviaryworker.go) & 
(cd main && go run aviarycoordinator.go) & 
(sleep 1 && cd main && go run clerk.go) && fg

# echo "hello" 
