
--SELECT *from TRANSACCION_TARJETA_CLIENTE;

VAR b_periodo NUMBER; -- se utiliza para definir el año a procesar (año anterior al actual)
EXEC :b_periodo := EXTRACT(YEAR FROM SYSDATE) -1;

DECLARE
--VARRAY PARA VALOR DE LOS TIPOS DE TRANSACCIONES,PERMITE RECORRER DINAMICAMENTE LOS TIPOS DE TRANSACCIONES QUE SERAN UTILIZADOS EN EL RESUMEN
TYPE tp_v_tipo_transaccion IS VARRAY(2) OF NUMBER; 
reg_tipo_transaccion tp_v_tipo_transaccion:= tp_v_tipo_transaccion(102,103);
exc_aporte_sbif EXCEPTION;
PRAGMA EXCEPTION_INIT (exc_aporte_sbif,-12838);
exc_cant_transacciones EXCEPTION;

--REGISTRO
TYPE tp_detalle IS RECORD  -- se utiliza para almacenar de forma temporal los datos de cada transaccion procesada antes de insertar en la tabla detalle
     (numrun NUMBER,
     dvrun VARCHAR2(1),
     nro_tarjeta NUMBER,
     fecha_transaccion DATE,
     monto_total_transaccion NUMBER)
     ;
--CURSOR EXPLICITO 
CURSOR cr_detalle IS  -- CURSOR PARA LA TABLA DETALLE
    SELECT 
        c.numrun,
        c.dvrun,
        tc.nro_tarjeta,
        ttc.nro_transaccion,
        TO_CHAR(ttc.fecha_transaccion,'DD/MM/YYYY') AS fecha_transaccion,
        ttt.nombre_tptran_tarjeta,
        ttc.monto_total_transaccion
        FROM TRANSACCION_TARJETA_CLIENTE ttc
        JOIN TARJETA_CLIENTE tc
        ON(ttc.nro_tarjeta = tc.nro_tarjeta)
        JOIN CLIENTE c
        ON(tc.numrun = c.numrun)
        JOIN TIPO_TRANSACCION_TARJETA ttt
        ON(ttt.cod_tptran_tarjeta = ttc.cod_tptran_tarjeta)
        WHERE ttc.cod_tptran_tarjeta IN (102,103) AND EXTRACT(YEAR FROM ttc.fecha_transaccion) = :b_periodo
        ORDER BY ttc.fecha_transaccion, c.numrun;

--CURSOR EXPLICITO CON PARAMETRO     
CURSOR cr_resumen(p_tipo NUMBER) IS -- CURSOR PARA LA TABLA RESUMEN
    SELECT
        TO_CHAR(ttc.fecha_transaccion,'MMYYYY') mes_anno,
        SUM(ttc.monto_total_transaccion) total_monto,
        ttt.nombre_tptran_tarjeta,
        SUM(ttc.monto_total_transaccion*tas.PORC_APORTE_SBIF/100) AS aporte_sbif
        FROM TRANSACCION_TARJETA_CLIENTE ttc 
        JOIN TIPO_TRANSACCION_TARJETA ttt
        ON(ttt.cod_tptran_tarjeta = ttc.cod_tptran_tarjeta)
        JOIN TRAMO_APORTE_SBIF tas
        ON (ttc.monto_total_transaccion BETWEEN tas.TRAMO_INF_AV_SAV AND tas.TRAMO_SUP_AV_SAV)
        WHERE ttt.cod_tptran_tarjeta = p_tipo AND EXTRACT(YEAR FROM ttc.fecha_transaccion) = :b_periodo
        GROUP BY TO_CHAR(ttc.fecha_transaccion,'MMYYYY'), ttt.nombre_tptran_tarjeta
        ORDER BY mes_anno;
        
--VARIABLES        
v_aporte_sbif NUMBER;
v_contador NUMBER := 0;
v_total NUMBER := 0;
v_detalle tp_detalle;
v_periodo NUMBER := :b_periodo;
v_cant_transacciones NUMBER;
        
BEGIN
    SELECT COUNT (*) 
    INTO v_cant_transacciones
    FROM TRANSACCION_TARJETA_CLIENTE ttc 
    WHERE ttc.cod_tptran_tarjeta IN (102,103) AND EXTRACT(YEAR FROM ttc.fecha_transaccion) = :b_periodo;
    
  --TRUNCAR EN TIEMPOS DE EJECUCION LAS TABLAS DETALLE Y RESUMEN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_APORTE_SBIF';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_APORTE_SBIF';

--RECORRIDO CURSOR  
FOR reg_detalle IN cr_detalle LOOP

    v_detalle.numrun := reg_detalle.numrun;
    v_detalle.dvrun := reg_detalle.dvrun;
    v_detalle.nro_tarjeta := reg_detalle.nro_tarjeta;
    v_detalle.fecha_transaccion := reg_detalle.fecha_transaccion;
    v_detalle.monto_total_transaccion := reg_detalle.monto_total_transaccion;
    
    --dbms_output.put_line('v_detalle.numrun:' ||reg_detalle.numrun);
    --dbms_output.put_line('v_detalle.dvrun:' ||reg_detalle.dvrun);
    --dbms_output.put_line('v_detalle.nro_tarjeta:' ||reg_detalle.nro_tarjeta);
    --dbms_output.put_line('v_detalle.nro_transaccion:' ||reg_detalle.nro_transaccion);
    --dbms_output.put_line('v_detalle.fecha_transaccion:' ||reg_detalle.fecha_transaccion);
   -- dbms_output.put_line('v_detalle.cod_tptran_tarjeta:' ||v_detalle.cod_tptran_tarjeta); 
    --dbms_output.put_line('v_detalle.monto_total_transaccion:' ||reg_detalle.monto_total_transaccion);
    
    SELECT PORC_APORTE_SBIF
        INTO v_aporte_sbif
        FROM TRAMO_APORTE_SBIF
        WHERE v_detalle.monto_total_transaccion BETWEEN TRAMO_INF_AV_SAV AND TRAMO_SUP_AV_SAV;

    v_aporte_sbif := ROUND(v_detalle.monto_total_transaccion * v_aporte_sbif / 100);
    
    INSERT INTO DETALLE_APORTE_SBIF VALUES (
        v_detalle.numrun,
        v_detalle.dvrun,
        v_detalle.nro_tarjeta,
        reg_detalle.nro_transaccion,
        v_detalle.fecha_transaccion,
        reg_detalle.NOMBRE_TPTRAN_TARJETA,
        v_detalle.monto_total_transaccion,
        v_aporte_sbif
    );
    
    v_contador:= v_contador +1;
    
END LOOP;

IF v_contador <> v_cant_transacciones THEN
    RAISE exc_cant_transacciones ;
  ELSE 
    COMMIT;
  END IF;
      
FOR i IN 1.. reg_tipo_transaccion.COUNT LOOP
    FOR r2 IN cr_resumen(reg_tipo_transaccion(i))LOOP
        INSERT INTO RESUMEN_APORTE_SBIF VALUES(
            r2.mes_anno,
            r2.nombre_tptran_tarjeta,
            r2.total_monto,
            ROUND(r2.aporte_sbif)
     );
  END LOOP;
END LOOP;
--EXCEPCION PREDEFINIDA POR ORACLE (EN EL CASO DE FORZAR EL ERROR NO_DATA_FOUND)
EXCEPTION
    WHEN NO_DATA_FOUND THEN 
        DBMS_OUTPUT.PUT_LINE('No se encuentraron datos para calcular el aporte SBIF');
        
--EXCEPCION NO PREDEFINIDA (SIN NOMBRE) POR FALTA DE COMMIT 
    WHEN exc_aporte_sbif THEN
            DBMS_OUTPUT.PUT_LINE('ERROR POR FALTA DE COMMIT'); 
--EXCEPCION DEFINIDA POR EL USUARIO / PARA SABER LA CANTIDAD DE TRANSACCIONES A PROCESAR
     WHEN exc_cant_transacciones THEN
        DBMS_OUTPUT.PUT_LINE('Cantidad de registros invalido');
        ROLLBACK;
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR DESCONOCIDO');
END;
/

--SET SERVEROUTPUT ON
    