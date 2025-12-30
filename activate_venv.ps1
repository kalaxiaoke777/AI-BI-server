Write-Host "正在激活venv虚拟环境..." -ForegroundColor Green
& .\venv\Scripts\Activate.ps1
Write-Host "venv环境已激活！" -ForegroundColor Green
Write-Host "Python版本: $(python --version)" -ForegroundColor Cyan
Write-Host "Python路径: $(python -c 'import sys; print(sys.executable)')" -ForegroundColor Cyan
