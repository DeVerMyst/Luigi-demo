from luigi import LocalTarget, S3Target

local_target = LocalTarget("/chemin/vers/mon/fichier.txt")
s3_target = S3Target("s3://mon-bucket/mon-fichier.txt")
