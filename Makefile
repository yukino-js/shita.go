.PHONY: commit
commit: config
	git add -A && git commit -m 'feat: Introduce new features'

.PHONY: feat
feat: commit
	git push origin main

.PHONY: config
config:
	git config user.name hangtiancheng && git config user.email '161043261@qq.com' 
