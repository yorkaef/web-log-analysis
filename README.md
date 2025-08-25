# 🚀 Iniciar Hadoop + Hive + Análisis (PowerShell)

Una vez instalado y configurado **Hadoop** y **Hive**, sigue estos pasos:

```powershell
# 1️⃣ Ingresar a WSL y abrir el proyecto
wsl
cd ~/web-log-analysis
code .

# 2️⃣ Inicializar HDFS y YARN
hdfs namenode -format
start-dfs.sh
start-yarn.sh
# 3️⃣ Iniciar Hive Metastore (en una terminal aparte)
hive --service metastore &
# 4️⃣ Verificar servicios de Hadoop
jps
# 5️⃣ Activar entorno virtual y ejecutar análisis completo
source venv/bin/activate
./run_complete_analysis.sh
