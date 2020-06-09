
activate-env:
	@echo Run "pyenv activate availability_monitor"

create-env:
	pyenv install 3.7.4
	pyenv virtualenv 3.7.4 availability_monitor

