#!/bin/bash
helm dependency update ./dex-base
helm dependency update ./airflow-test
helm dependency update ./spark-test
helm package dex-base airflow-test spark-test
mv dex-base*.tgz spark-test*.tgz airflow-test*.tgz ../eks-charts/
cd ..
helm repo index ./eks-charts --url https://arunmahadevan.github.io/eks-charts
git add eks-charts
cat<<EOF
Please check the changes and:
 git commit -m "changes"
 git push
EOF
