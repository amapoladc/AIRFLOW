# etl_colombia
ETL: Colombia
# GitFlow Workflow - Guía de Uso

Este repositorio utiliza **GitFlow**, una metodología de ramificación para gestionar el desarrollo de software de manera organizada y eficiente. Sigue esta guía para entender cómo trabajar con las ramas en este repositorio.

---

## Estructura de ramas

1. **`main`**
   - Contiene el código listo para producción.
   - Solo se hacen merges desde `release` o `hotfix`.

2. **`develop`**
   - Contiene el código más reciente que está en desarrollo.
   - Se integra código desde las ramas `feature`.

3. **`feature/nombre-feature`**
   - Se crean desde `develop`.
   - Usadas para desarrollar nuevas funcionalidades.

4. **`release/version-x.x`**
   - Se crean desde `develop`.
   - Usadas para preparar una nueva versión para producción.

5. **`hotfix/nombre-hotfix`**
   - Se crean desde `main`.
   - Usadas para corregir errores críticos en producción.

---

## Flujo de trabajo

### 1. Desarrollar una nueva funcionalidad

1. Cambia a la rama `develop`:
   ```bash
   git checkout develop
   ```

2. Crea una rama `feature`:
   ```bash
   git checkout -b feature/nombre-feature
   ```

3. Realiza los cambios necesarios, agrega los archivos y haz commit:
   ```bash
   git add .
   git commit -m "Descripción de la nueva funcionalidad"
   ```

4. Cuando finalices, haz merge con `develop`:
   ```bash
   git checkout develop
   git merge feature/nombre-feature
   git branch -d feature/nombre-feature
   ```

5. Sube los cambios a remoto:
   ```bash
   git push origin develop
   ```

### 2. Preparar una versión para producción

1. Cambia a la rama `develop`:
   ```bash
   git checkout develop
   ```

2. Crea una rama `release`:
   ```bash
   git checkout -b release/version-x.x
   ```

3. Realiza ajustes finales, pruebas o documentación.

4. Fusiona la rama `release` con `main` para desplegar:
   ```bash
   git checkout main
   git merge release/version-x.x
   git push origin main
   ```

5. Fusiona la rama `release` con `develop` para mantener consistencia:
   ```bash
   git checkout develop
   git merge release/version-x.x
   git push origin develop
   ```

6. Elimina la rama local y en remoto:
   ```bash
   git branch -d release/version-x.x
   git push origin --delete release/version-x.x
   ```

### 3. Corregir un error crítico en producción

1. Cambia a la rama `main`:
   ```bash
   git checkout main
   ```

2. Crea una rama `hotfix`:
   ```bash
   git checkout -b hotfix/nombre-hotfix
   ```

3. Realiza los cambios necesarios, agrega los archivos y haz commit:
   ```bash
   git add .
   git commit -m "Corrección crítica: descripción del problema"
   ```

4. Fusiona la rama `hotfix` con `main` para desplegar:
   ```bash
   git checkout main
   git merge hotfix/nombre-hotfix
   git push origin main
   ```

5. Fusiona la rama `hotfix` con `develop` para mantener consistencia:
   ```bash
   git checkout develop
   git merge hotfix/nombre-hotfix
   git push origin develop
   ```

6. Elimina la rama local y en remoto:
   ```bash
   git branch -d hotfix/nombre-hotfix
   git push origin --delete hotfix/nombre-hotfix
   ```

---

## Configuración inicial

1. Inicializa el repositorio (si aún no está inicializado):
   ```bash
   git init
   ```

2. Agrega el repositorio remoto:
   ```bash
   git remote add origin <URL-del-repositorio>
   ```

3. Crea las ramas principales:
   ```bash
   git checkout -b main
   git push origin main

   git checkout -b develop
   git push origin develop
   ```

4. Protege las ramas críticas (`main` y `develop`) en la configuración del repositorio para evitar commits directos.

---

## Reglas de nomenclatura

- **Ramas de funcionalidad**: `feature/nombre-feature`
- **Ramas de versión**: `release/version-x.x`
- **Ramas de corrección**: `hotfix/nombre-hotfix`

---

## Notas

- Siempre actualiza tus ramas antes de realizar un merge:
  ```bash
  git pull origin develop
  ```
- Realiza revisiones de código antes de integrar cambios en `develop` o `main`.
