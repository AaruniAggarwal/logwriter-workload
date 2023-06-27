# logwriter-workload
This has the logwriter workload app files which involves both log writing and log reading python scripts. Also manifests for both CephFS and RBD deploynments/statefulsets. Also the docker container file.
Make sure to create logwriter.cephfs.yaml in test-project namespace as both logwriter.cephfs.yaml and logreader.cephfs.yaml makes use of same PVC ie. logwriter-cephfs-many
