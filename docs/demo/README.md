# docs/demo/ — Video demostrativo

El video de la entrega (`YeisonForero_SaraLeon_DafneSuspes_video.mp4`) está en el OneDrive institucional de la Universidad Santo Tomás. El enlace está en el README principal, sección 8 ("Componentes entregables").

Recorrido de la demo en vivo para la sustentación:

1. **Red:** abrir `network/firewatch.pkt`; `show ip ospf neighbor` y `show ip route` en R-TUNJA; ping de PC-SOG-1 a SRV-BI; acceso por nombre a SRV-BI desde PC-DUI-1.
2. **Pipeline:** con el `.venv` activo y Java 17, `python scripts/run_pipeline.py --stage gold` (requiere silver y gold de OneDrive en `data/`). Debe terminar con la conciliación K1 = 3.908 en OK.
3. **Benchmarks:** figuras de `benchmarks/figures/` (speedup y eficiencia de `multiprocessing` y de Spark).
4. **Dashboard:** abrir `dashboard/firewatch.pbix` con `RutaGold` apuntando a `data\gold\`; recorrer las 3 páginas y usar un segmentador y un drill-down.

Respaldo si algo falla en vivo: el video, las capturas de `dashboard/capturas/` y `network/evidencias/`, y los informes de `docs/evidencias/`.
