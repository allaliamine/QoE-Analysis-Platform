#!/usr/bin/env bash
kubectl port-forward -n qoe svc/frontend 8090:80
